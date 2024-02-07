import glob
import hashlib
from huggingface_hub import snapshot_download
import json
import os

CachedFileStats = {
    "checksum": str,
    "file": str,
}

class DatasetAdapter():

    def __init__(self, id: str, storage_dir: str) -> None:
        """Hugging Face Info Initialization
        Args:
            id: The id of the dataset in Hugging Face
        """
        self.id = id
        repo_dir = id.replace("/", "-")
        self.root_dir =  os.path.join(storage_dir, repo_dir)
        self.cache_dir = os.path.join(self.root_dir, "cache")
        self.local_dir = os.path.join(self.root_dir, "current")
        self.loaded = False

    def list_files(self) -> list[str]:
        if not self.loaded:
            self.load()
            self.loaded = True
        
        all_paths = glob.glob(self.local_dir + '/**/*', recursive=True)
        
        return list(filter(os.path.isfile, all_paths))

    def calculate_checksum(self, file: str) -> str:
        with os.open(file) as file:
            file_hash = hashlib.md5()
            while chunk := file.read(8192):
                file_hash.update(chunk)
            
            return file_hash.digest()
        
    def verify_local_file(self, file: str, checksum: str, local_files: list[str]) -> bool:
        local_file_exists = file in local_files
        
        if local_file_exists:
            file_path = os.path.join(self.local_dir, file)
            current_checksum = self.calculate_checksum(file_path)
            return current_checksum == checksum
        
        return False
    
    def verify_local_files(self, expected_files: list[CachedFileStats], local_files: list[str]) -> bool:
        
        for expected_file in expected_files:
            file = expected_file["file"]
            if (not os.path.isdir(file)) and (not self.verify_local_file(file, expected_file["checksum"], local_files)):
                return False
        
        return True
        
    def verify_local_stats(self) -> bool:
        stats_path = os.path.join(self.root_dir, "stats.json")
        try:
            with os.open(stats_path) as stats_file:
                data = json.load(stats_file)
                
                expected_files = data['files']
                local_files = self.list_files()
                return self.verify_local_files(expected_files, local_files)
        except:
            return False
        
    def stat_download(self):
        local_files = self.list_files()

    def load(self):
        
        
        if self.verify_local_stats():
            print("Using local cached version of dataset")
            
        else:
            print("Downloading fresh dataset")
            snapshot_download(
                repo_id=self.id,
                repo_type="dataset",
                cache_dir=self.cache_dir,
                local_dir=self.local_dir,
                resume_download=True,
                force_download=True,
            )

        self.loaded = True


        