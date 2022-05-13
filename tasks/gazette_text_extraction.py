import logging
import tempfile
import os
from pathlib import Path
from typing import Dict, Iterable

from .interfaces import StorageInterface, IndexInterface, TextExtractorInterface


def extract_text_from_gazettes(
    gazettes: Iterable[Dict],
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> Iterable[Dict]:
    """
    Extracts the text from a list of gazettes
    """
    logging.info("Starting text extraction from gazettes")
    create_index(index)
    for gazette in gazettes:
        try:
            processed_gazette = try_process_gazette_file(
                gazette, storage, index, text_extractor
            )
        except Exception as e:
            logging.warning(
                f"Could not process gazette: {gazette['file_path']}. Cause: {e}"
            )
        else:
            yield processed_gazette


def try_process_gazette_file(
    gazette: Dict,
    storage: StorageInterface,
    index: IndexInterface,
    text_extractor: TextExtractorInterface,
) -> Dict:
    """
    Do all the work to extract the content from the gazette files
    """
    logging.debug(f"Processing gazette {gazette['file_path']}")
    # gazette_file = download_gazette_file(gazette, storage)
    # get_gazette_text_and_define_url(gazette, gazette_file, text_extractor)
    # upload_gazette_raw_text(gazette, storage)
    # index.index_document(gazette, document_id=gazette["file_checksum"])
    # delete_gazette_files(gazette_file)
    return gazette


def create_index(index: IndexInterface) -> None:
    body = {
        "mappings": {
            "properties": {
                "created_at": {"type": "date"},
                "date": {"type": "date"},
                "edition_number": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "file_checksum": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "file_path": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "file_url": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "id": {"type": "long"},
                "is_extra_edition": {"type": "boolean"},
                "power": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "processed": {"type": "boolean"},
                "scraped_at": {"type": "date"},
                "source_text": {
                    "type": "text",
                    "index_options": "offsets",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "state_code": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "territory_id": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "territory_name": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
                "url": {
                    "type": "text",
                    "fields": {"keyword": {"type": "keyword", "ignore_above": 256}},
                },
            }
        }
    }
    index.create_index(body=body)


def upload_gazette_raw_text(gazette: Dict, storage):
    """
    Define gazette raw text
    """
    file_raw_txt = Path(gazette["file_path"]).with_suffix(".txt").as_posix()
    storage.upload_content(file_raw_txt, gazette["source_text"])
    logging.debug(f"file_raw_txt uploaded {file_raw_txt}")
    file_endpoint = get_file_endpoint()
    gazette["file_raw_txt"] = f"{file_endpoint}/{file_raw_txt}"


def get_gazette_text_and_define_url(
    gazette: Dict, gazette_file: str, text_extractor: TextExtractorInterface
):
    """
    Extract file content and define the url to access the file in the storage
    """
    gazette["source_text"] = try_to_extract_content(gazette_file, text_extractor)
    file_endpoint = get_file_endpoint()
    gazette["url"] = f"{file_endpoint}/{gazette['file_path']}"


def get_file_endpoint() -> str:
    """
    Get the endpoint where the gazette files can be downloaded.
    """
    return os.environ["QUERIDO_DIARIO_FILES_ENDPOINT"]


def try_to_extract_content(
    gazette_file: str, text_extractor: TextExtractorInterface
) -> str:
    """
    Calls the function to extract the content from the gazette file. If it fails
    remove the gazette file and raise an exception
    """
    try:
        return text_extractor.extract_text(gazette_file)
    except Exception as e:
        os.remove(gazette_file)
        raise e


def delete_gazette_files(gazette_file: str) -> None:
    """
    Removes the files used to process the gazette content.
    """
    os.remove(gazette_file)


def download_gazette_file(gazette, storage: StorageInterface) -> str:
    """
    Download the file from the object storage and write it down in the local
    disk to allow the text extraction
    """
    with tempfile.NamedTemporaryFile(delete=False) as tmpfile:
        gazette_file_key = get_gazette_file_key_used_in_storage(gazette)
        storage.get_file(gazette_file_key, tmpfile)
        return tmpfile.name


def get_gazette_file_key_used_in_storage(gazette) -> str:
    """
    Get the file key used to store the gazette in the object storage
    """
    return gazette["file_path"]


def set_gazette_as_processed(self, id: int, gazette_file_checksum: str) -> None:
    logging.debug(f"Marking {id}({gazette_file_checksum}) as processed")
    with self._connection.cursor() as cursor:
        cursor.execute(
            self.UPDATE_GAZETTE_AS_PROCESSED,
            {"id": id, "file_checksum": gazette_file_checksum},
        )
        self._connection.commit()
