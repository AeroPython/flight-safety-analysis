import urllib.request
import os
from tqdm import tqdm


def download_data(url, destination_folder="../data", name='avall.db', force=False):
    
    path = os.path.join(destination_folder, name)

    if not os.path.isfile(path) or force:
        print('Downloading...')
        with TqdmUpTo(unit='B', unit_scale=True, miniters=1) as t:
            urllib.request.urlretrieve(url, path, reporthook=t.update_to)
    else:
        print("Data has already been downloaded")


class TqdmUpTo(tqdm):
    """Provides `update_to(n)` which uses `tqdm.update(delta_n)`."""
    def update_to(self, b=1, bsize=1, tsize=None):
        """
        b  : int, optional
            Number of blocks transferred so far [default: 1].
        bsize  : int, optional
            Size of each block (in tqdm units) [default: 1].
        tsize  : int, optional
            Total size (in tqdm units). If [default: None] remains unchanged.
        """
        if tsize is not None:
            self.total = tsize
        self.update(b * bsize - self.n)  # will also set self.n = b * bsize



if __name__ == '__main__':
    
    URL = "https://www.dropbox.com/s/n9inalri0dvff1j/avall.db?dl=1"
    download_data(URL, force=True)
