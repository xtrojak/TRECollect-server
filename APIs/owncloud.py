import base64
import os
import requests
from datetime import datetime
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree


class OwnCloudAPI:
    def __init__(self):
        self.owncloud_url = os.environ["OWCLOUD_URL"]
        self.submissions_token = os.environ["OWCLOUD_SUBMISSIONS_TOKEN"]
        self.configs_token = os.environ["OWCLOUD_CONFIGS_TOKEN"]
        self.backups_token = os.environ["OWCLOUD_BACKUPS_TOKEN"]

    def _auth_headers(self, token_type="submissions"):
        """Basic auth header for OwnCloud: Base64(accessToken + ':'). Used for all requests.

        Args:
            token_type (str): One of "submissions", "configs", "backups". Default "submissions".
        """
        tokens = {
            "submissions": self.submissions_token,
            "configs": self.configs_token,
            "backups": self.backups_token,
        }
        token = tokens.get(token_type, self.submissions_token)
        credentials = f"{token}:"
        encoded = base64.b64encode(credentials.encode()).decode()
        return {"Authorization": f"Basic {encoded}"}

    def _propfind_with_props(self, remote_path="", depth="1", props=None, token_type="submissions"):
        """Run PROPFIND with Basic auth. By default requests getlastmodified and resourcetype."""
        url = f"{self.owncloud_url}/{remote_path}".rstrip("/") if remote_path else self.owncloud_url.rstrip("/")
        headers = {
            "Depth": depth,
            "Content-Type": "application/xml",
            **self._auth_headers(token_type=token_type),
        }
        if props is None:
            props = "<d:getlastmodified/><d:resourcetype/>"
        body = "<?xml version=\"1.0\"?>\n<d:propfind xmlns:d=\"DAV:\">\n  <d:prop>" + props + "</d:prop>\n</d:propfind>"
        response = requests.request("PROPFIND", url, headers=headers, data=body)
        if response.status_code not in (207, 200):
            raise RuntimeError(f"PROPFIND failed: {response.status_code} - {response.text}")
        return response.content

    def _list_modified_collections(self, remote_path: str, last_check_utc) -> list[tuple[str, Optional[datetime]]]:
        """List direct child folders whose getlastmodified is after last_check_utc. Returns (child_name, mod_dt)."""
        raw = self._propfind_with_props(remote_path, depth="1")
        tree = ElementTree.fromstring(raw)
        ns = {"d": "DAV:"}
        url = f"{self.owncloud_url}/{remote_path}".rstrip("/") if remote_path else self.owncloud_url.rstrip("/")
        base_path = "/".join(p for p in urlparse(url).path.split("/") if p) or ""
        if base_path and not base_path.startswith("/"):
            base_path = "/" + base_path

        result = []
        for resp in tree.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue
            raw_href = href_el.text.strip()
            href_path = unquote(urlparse(raw_href).path).rstrip("/")
            # Normalize: collapse multiple slashes so we match server hrefs regardless of trailing slashes in URL
            href_path = "/" + "/".join(p for p in href_path.split("/") if p) if href_path else ""
            # Server may return relative hrefs for subfolders (e.g. "LSI/" instead of full path)
            if href_path and not href_path.startswith("/"):
                href_path = "/" + base_path.lstrip("/") + "/" + href_path.lstrip("/")
            if not href_path:
                continue
            prefix = (base_path.rstrip("/") + "/") if base_path else "/"
            if href_path.rstrip("/") == base_path.rstrip("/"):
                continue
            if not href_path.startswith(prefix):
                # Server may return relative href (e.g. "LSI/" or "/LSI" meaning child of requested folder)
                segment = href_path.strip("/").split("/")[0] if href_path.strip("/") else ""
                if not segment or "/" in href_path.strip("/"):
                    continue
                child_name = segment
            else:
                rel = href_path[len(prefix):].lstrip("/")
                child_name = rel.split("/")[0]
                if "/" in rel:
                    continue

            for propstat in resp.findall("d:propstat", ns):
                status = propstat.find("d:status", ns)
                if status is None or status.text is None or "200" not in status.text:
                    continue
                prop = propstat.find("d:prop", ns)
                if prop is None:
                    continue
                resourcetype = prop.find("d:resourcetype", ns)
                if resourcetype is None or resourcetype.find("d:collection", ns) is None:
                    continue

                mod_dt = None
                mod_el = prop.find("d:getlastmodified", ns)
                if mod_el is not None and mod_el.text:
                    try:
                        parsed = parsedate_to_datetime(mod_el.text.strip())
                        # Compare using naive datetimes (drop timezone info if present)
                        mod_dt = parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
                    except (ValueError, TypeError):
                        pass
                if mod_dt is None or mod_dt > last_check_utc:
                    result.append((child_name, mod_dt))
                break
        return result

    def get_new_folders(self, last_check):
        """Return full paths to modified site folders (lowest level) under root.

        Uses Depth: 1 at each level; only recurses into folders whose getlastmodified
        is after last_check, so we skip unchanged branches and reduce the number of
        requests. Structure: root -> hash -> (LSI | AML | logs) -> subteam -> site.
        We ignore "logs". Subteam names are arbitrary.

        Args:
            last_check (datetime): only folders modified after this are considered.

        Returns:
            list[str]: full paths like "hash1/LSI/subteam1/site1", "hash2/AML/foo/siteN", etc.
        """
        if last_check.tzinfo is not None:
            last_check_utc = last_check.astimezone().replace(tzinfo=None)
        else:
            last_check_utc = last_check
        result = []

        for hash_name, _ in self._list_modified_collections("", last_check_utc):
            hash_path = hash_name
            for team_name, _ in self._list_modified_collections(hash_path, last_check_utc):
                if team_name == "logs":
                    continue
                team_path = f"{hash_path}/{team_name}"
                for subteam_name, _ in self._list_modified_collections(team_path, last_check_utc):
                    subteam_path = f"{team_path}/{subteam_name}"
                    for site_name, _ in self._list_modified_collections(subteam_path, last_check_utc):
                        result.append(f"{subteam_path}/{site_name}")
        return result

    def get_new_config_files(self, remote_root: str, local_root: str, last_check: datetime) -> list[str]:
        """Download JSON config files that are not yet present locally.

        The OwnCloud structure is flat: remote_root/<config_name>/<version>.json.
        We list all config folders and JSON files using PROPFIND (Depth 1) with the
        configs token, and only download files that do not exist in the corresponding
        local subfolder. The last_check timestamp is accepted for future use but is
        not currently used in this simplified implementation.
        """
        import os
        os.makedirs(local_root, exist_ok=True)

        # First list config folders under remote_root (Depth 1, collections only).
        # Normalize slashes so we don't end up with double '/' when owncloud_url already ends with one.
        raw = self._propfind_with_props(remote_root, depth="1", token_type="configs")
        tree = ElementTree.fromstring(raw)
        ns = {"d": "DAV:"}
        base_path = urlparse(
            f"{self.owncloud_url.rstrip('/')}/{remote_root.lstrip('/')}".rstrip("/")
        ).path.rstrip("/")
        folder_names: list[str] = []

        for resp in tree.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue
            href_path = unquote(urlparse(href_el.text.strip()).path).rstrip("/")
            if href_path == base_path or not href_path.startswith(base_path + "/"):
                continue
            child_rel = href_path[len(base_path) + 1 :]
            if "/" in child_rel or not child_rel:
                continue
            # Must be a collection (folder)
            propstat = resp.find("d:propstat", ns)
            if propstat is None:
                continue
            prop = propstat.find("d:prop", ns)
            if prop is None:
                continue
            resourcetype = prop.find("d:resourcetype", ns)
            if resourcetype is None or resourcetype.find("d:collection", ns) is None:
                continue
            folder_names.append(child_rel)

        downloaded: list[str] = []

        # If there are no subfolders, treat remote_root itself as the flat folder
        if not folder_names:
            folder_names = [""]

        # For each config folder (or remote_root itself), list JSON files and download only those not present locally
        for folder in folder_names:
            if folder:
                folder_remote = f"{remote_root.rstrip('/')}/{folder}"
                local_folder = os.path.join(local_root, folder)
            else:
                folder_remote = remote_root.rstrip("/")
                local_folder = local_root

            os.makedirs(local_folder, exist_ok=True)

            raw = self._propfind_with_props(folder_remote, depth="1", token_type="configs")
            tree = ElementTree.fromstring(raw)
            for resp in tree.findall("d:response", ns):
                href_el = resp.find("d:href", ns)
                if href_el is None or href_el.text is None:
                    continue
                href_path = unquote(urlparse(href_el.text.strip()).path).rstrip("/")
                base_folder_path = urlparse(
                    f"{self.owncloud_url.rstrip('/')}/{folder_remote.lstrip('/')}".rstrip("/")
                ).path.rstrip("/")
                if href_path == base_folder_path or not href_path.startswith(base_folder_path + "/"):
                    continue
                filename = href_path[len(base_folder_path) + 1 :]
                if "/" in filename or not filename.lower().endswith(".json"):
                    continue

                file_url = f"{self.owncloud_url}/{folder_remote}/{filename}"
                r = requests.get(file_url, headers=self._auth_headers(token_type="configs"))
                if r.status_code not in (200, 201, 204):
                    print(f"{r.status_code} - {r.text} (skipping {file_url})")
                    continue
                local_path = os.path.join(local_folder, filename)
                if os.path.exists(local_path):
                    continue
                with open(local_path, "w", encoding="utf-8") as f:
                    f.write(r.text)
                downloaded.append(local_path)

        return downloaded

    def get_remote_files(self, remote_path):
        """Download all XML files from a remote directory (flat list, no subfolders).

        PROPFIND lists direct children; we keep only non-collection items whose name
        ends with .xml, then GET each file with submissions_token and return
        (filename, content) pairs.

        Args:
            remote_path (str): target remote directory.

        Returns:
            list of tuple: [(filename, content), ...] for each .xml file.
        """
        raw = self._propfind_with_props(remote_path, depth="1")
        tree = ElementTree.fromstring(raw)
        ns = {"d": "DAV:"}
        base_path = urlparse(f"{self.owncloud_url}{remote_path}".rstrip("/")).path.rstrip("/")
        base_url = f"{self.owncloud_url}{remote_path}".rstrip("/")
        files_to_download = []

        for resp in tree.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue
            href_path = unquote(urlparse(href_el.text.strip()).path).rstrip("/")
            if href_path == base_path:
                continue
            if not href_path.startswith(base_path + "/"):
                continue
            child_rel = href_path[len(base_path) + 1 :]
            if "/" in child_rel:
                continue
            if not child_rel.lower().endswith(".xml"):
                continue

            propstat = resp.find("d:propstat", ns)
            if propstat is None:
                continue
            prop = propstat.find("d:prop", ns)
            if prop is None:
                continue
            resourcetype = prop.find("d:resourcetype", ns)
            if resourcetype is not None and resourcetype.find("d:collection", ns) is not None:
                continue  # skip folders

            files_to_download.append(child_rel)

        result = []
        for filename in files_to_download:
            file_url = f"{base_url}/{filename}"
            r = requests.get(file_url, headers=self._auth_headers())
            if r.status_code in (200, 201, 204):
                result.append((filename, r.text))
            else:
                print(f"{r.status_code} - {r.text} (skipping {filename})")
        return result

    def upload_file(self, remote_path, bytes):
        """Upload a local file to the OwnCloud destination

        Args:
            remote_path (str): destination path within OwnCloud

        Returns:
            bool: True if successful
        """
        response = requests.put(f'{self.owncloud_url}/{remote_path}', data=bytes, headers=self._auth_headers())
        success = response.status_code in [200, 201, 204]
        if not success:
            print({response.status_code} - {response.text})
        return success
    
    def download_file(self, remote_path, file_type='txt'):
        """Download txt file form remote path

        Args:
            remote_path (str): location of remote file
            file_type (str): type of file to download

        Returns:
            str: content of the file
        """
        response = requests.get(f'{self.owncloud_url}/{remote_path}', headers=self._auth_headers())
        success = response.status_code in [200, 201, 204]
        if not success:
            print({response.status_code} - {response.text})
        else:
            if file_type == 'txt':
                return response.text
            elif file_type == 'json':
                return response.json()
            else:
                raise ValueError(f'Invalid file type: {file_type}')
