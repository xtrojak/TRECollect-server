import time
import functools
import threading
from gspread.exceptions import APIError
import os
import math

def clean_up_nulls(values):
    return ["" if x is None or x == "nan" or (isinstance(x, float) and math.isnan(x)) else x for x in values]


def create_keyfile_dict():
    """Build dict of Google credentials from env variables

    Returns:
        dict: credentials
    """
    variables_keys = {
        'type': os.environ.get('SHEET_TYPE'),
        'project_id': os.environ.get('SHEET_PROJECT_ID'),
        'private_key_id': os.environ.get('SHEET_PRIVATE_KEY_ID'),
        'private_key': os.environ.get('SHEET_PRIVATE_KEY').replace('\\n', '\n'),
        'client_email': os.environ.get('SHEET_CLIENT_EMAIL'),
        'client_id': os.environ.get('SHEET_CLIENT_ID'),
        'auth_uri': os.environ.get('SHEET_AUTH_URI'),
        'token_uri': os.environ.get('SHEET_TOKEN_URI'),
        'auth_provider_x509_cert_url': os.environ.get('SHEET_AUTH_PROVIDER_X509_CERT_URL'),
        'client_x509_cert_url': os.environ.get('SHEET_CLIENT_X509_CERT_URL'),
        'universe_domain': os.environ.get('UNIVERSE_DOMAIN')
    }
    return variables_keys



class GlobalRateLimiter:
    """Global rate limiter that ensures API calls are spaced out across all methods."""
    
    def __init__(self, delay=2.0):
        self.delay = delay
        self.last_call_time = 0
        self.lock = threading.Lock()
    
    def wait_if_needed(self):
        """Wait if necessary to maintain the rate limit."""
        with self.lock:
            current_time = time.time()
            time_since_last_call = current_time - self.last_call_time
            
            if time_since_last_call < self.delay:
                sleep_time = self.delay - time_since_last_call
                time.sleep(sleep_time)
            
            self.last_call_time = time.time()


# Global instance for the GoogleAPI class
_global_rate_limiter = GlobalRateLimiter(delay=2.0)


def rate_limited_with_retry(delay=2, max_retries=7):
    """Decorator that combines global rate limiting with retry logic for API errors.
    
    Args:
        delay (float): Delay between calls in seconds (used to set global rate limiter)
        max_retries (int): Maximum number of retry attempts for API errors
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Update global rate limiter delay if different
            if _global_rate_limiter.delay != delay:
                _global_rate_limiter.delay = delay
            
            current_delay = delay  # Create a local variable that can be modified
            for attempt in range(max_retries + 1):
                # Use global rate limiter to ensure proper spacing between all API calls
                _global_rate_limiter.wait_if_needed()
                
                try:
                    return func(*args, **kwargs)
                except APIError as e:
                    print(e)
                    if attempt < max_retries:
                        current_delay *= 2  # exponential backoff
                        print(f"!!! Rate limit hit. Retrying in {current_delay:.1f} seconds...")
                    else:
                        # Last attempt failed, raise the error
                        raise e
        return wrapper
    return decorator
