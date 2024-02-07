from tokenizer.huggingface.dataset_adapter import DatasetAdapter

class Boot():
    def __init__(self) -> None:

        dataset_info = DatasetAdapter("monology/pile-uncopyrighted", "/Volumes/Games/Temp/tokenizier")
        print(dataset_info.list_files())


Boot()