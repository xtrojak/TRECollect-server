import os
import requests
from urllib.parse import unquote, urlparse
from xml.etree import ElementTree


class OwnCloudAPI:
    def __init__(self, subfolder):
        self.owncloud_url = os.environ["OWCLOUD_URL"]
        self.submissions_token = os.environ["OWCLOUD_SUBMISSIONS_TOKEN"]
        self.configs_token = os.environ["OWCLOUD_CONFIGS_TOKEN"]
        self.backups_token = os.environ["OWCLOUD_BACKUPS_TOKEN"]

    def _propfind_with_props(self, remote_path, depth="1", props=None):
        """Run PROPFIND with Bearer token. By default requests getlastmodified and resourcetype.
        Pass props to request only getetag+resourcetype for a minimal response.
        """
        url = f"{self.owncloud_url}/{remote_path}".rstrip("/")
        headers = {
            "Depth": depth,
            "Authorization": f"Bearer {self.submissions_token}",
            "Content-Type": "application/xml",
        }
        if props is None:
            props = "<d:getlastmodified/><d:resourcetype/>"
        body = "<?xml version=\"1.0\"?>\n<d:propfind xmlns:d=\"DAV:\">\n  <d:prop>" + props + "</d:prop>\n</d:propfind>"
        response = requests.request("PROPFIND", url, headers=headers, data=body)
        if response.status_code not in (207, 200):
            raise RuntimeError(f"PROPFIND failed: {response.status_code} - {response.text}")
        return response.content

    def _propfind_folders_etag(self, remote_path):
        """PROPFIND Depth 1 with only getetag and resourcetype for minimal payload."""
        return self._propfind_with_props(
            remote_path, depth="1", props="<d:getetag/><d:resourcetype/>"
        )

    def _get_folder_etag(self, remote_path):
        """Get the ETag of the folder itself (Depth 0, getetag only). Returns None if missing."""
        raw = self._propfind_with_props(
            remote_path, depth="0", props="<d:getetag/>"
        )
        tree = ElementTree.fromstring(raw)
        ns = {"d": "DAV:"}
        for resp in tree.findall("d:response", ns):
            for propstat in resp.findall("d:propstat", ns):
                status = propstat.find("d:status", ns)
                if status is None or status.text is None or "200" not in status.text:
                    continue
                prop = propstat.find("d:prop", ns)
                if prop is None:
                    continue
                etag_el = prop.find("d:getetag", ns)
                if etag_el is not None and etag_el.text:
                    return etag_el.text.strip()
        return None

    def upload_file(self, remote_path, bytes):
        """Upload a local file to the OwnCloud destination

        Args:
            remote_path (str): destination path within OwnCloud

        Returns:
            bool: True if successful
        """
        response = requests.put(f'{self.owncloud_url}/{remote_path}', data=bytes)
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
        response = requests.get(f'{self.owncloud_url}/{remote_path}')
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

    def get_new_folders(self, remote_path, previous_folder_etag=None):
        """Use the target folder's ETag to skip work when nothing changed; otherwise list subfolders.

        Store only one ETag (for the whole target folder, e.g. submissions). First we run a
        minimal PROPFIND Depth 0 with getetag only. If the folder ETag equals previous_folder_etag,
        we return ([], current_etag) and the caller can skip listing and processing. If it
        changed (or no previous stored), we PROPFIND Depth 1 to list all direct subfolders and
        return (subfolder_names, current_etag). Caller should persist current_etag (e.g. in the
        same config as last_check_timestamp) for the next run.

        Args:
            remote_path (str): target remote folder (e.g. "submissions").
            previous_folder_etag (str | None): ETag from last run; if None, we always list.

        Returns:
            tuple: (subfolder_names: list[str], current_folder_etag: str | None).
                When nothing changed, subfolder_names is empty. When changed, all direct subfolder names.
        """
        current_etag = self._get_folder_etag(remote_path)
        if current_etag is None:
            # Can't get etag; fall back to listing subfolders
            subfolders = self._list_subfolders(remote_path)
            return subfolders, None
        if previous_folder_etag is not None and current_etag == previous_folder_etag:
            return [], current_etag
        subfolders = self._list_subfolders(remote_path)
        return subfolders, current_etag

    def _list_subfolders(self, remote_path):
        """List direct subfolder names under remote_path (collections only)."""
        raw = self._propfind_folders_etag(remote_path)
        tree = ElementTree.fromstring(raw)
        ns = {"d": "DAV:"}
        base_path = urlparse(f"{self.owncloud_url}/{remote_path}".rstrip("/")).path.rstrip("/")
        names = []
        for resp in tree.findall("d:response", ns):
            href_el = resp.find("d:href", ns)
            if href_el is None or href_el.text is None:
                continue
            href_path = unquote(urlparse(href_el.text.strip()).path).rstrip("/")
            if href_path == base_path or not href_path.startswith(base_path + "/"):
                continue
            child_name = href_path[len(base_path) + 1 :].split("/")[0]
            if "/" in href_path[len(base_path) + 1 :]:
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
                names.append(child_name)
                break
        return names

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
        base_path = urlparse(f"{self.owncloud_url}/{remote_path}".rstrip("/")).path.rstrip("/")
        base_url = f"{self.owncloud_url}/{remote_path}".rstrip("/")
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
        headers = {"Authorization": f"Bearer {self.submissions_token}"}
        for filename in files_to_download:
            file_url = f"{base_url}/{filename}"
            r = requests.get(file_url, headers=headers)
            if r.status_code in (200, 201, 204):
                result.append((filename, r.text))
            else:
                print(f"{r.status_code} - {r.text} (skipping {filename})")
        return result

    def get_remote_folders(self, remote_path):
        """Inspect remote folder and get all subfolders

        Args:
            remote_path (str): target remote destination

        Returns:
            list: list of subfolders
        """
        headers = {
            "Depth": "1"  # "1" lists contents (not recursive)
        }
        response = requests.request("PROPFIND", f'{self.owncloud_url}/{remote_path}', 
                                    headers=headers)
        
        if response.status_code not in (207, 200):
            print(f'{response.status_code} - {response.text}')
        else:
            tree = ElementTree.fromstring(response.content)

            namespace = {"d": "DAV:"}
            files = [
                elem.find("d:href", namespace).text
                for elem in tree.findall("d:response", namespace)
            ]

            return [unquote(urlparse(file_url).path).split('/')[-2] for file_url in files]
        
    def check_create_folder(self, folder_name, remote_path=''):
        """Create new folder in OwnCloud

        Response code 405 is acceptable (folder already exists)

        Args:
            folder_name (str): name of the folder to be created
            remote_path (str, optional): destination path within OwnCloud. Defaults to ''.

        Returns:
            bool: True if successful
        """
        if remote_path:
            folder_name = f'{remote_path}/{folder_name}'

        # Create folder using MKCOL request
        response = requests.request("MKCOL", f'{self.owncloud_url}/{folder_name}')
        
        success = response.status_code in [201, 405]
        if not success:
            print({response.status_code} - {response.text})
        return success
