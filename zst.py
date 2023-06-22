import zstandard
import os
import json
from datetime import datetime
import decouple
import logging.handlers


log = logging.getLogger("bot")
log.setLevel(logging.DEBUG)
log.addHandler(logging.StreamHandler())


def read_and_decode(reader, chunk_size, max_window_size, previous_chunk=None, bytes_read=0):
    """
    Used by @read_lines_zst.
    Borrowed from
        https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py
    """
    chunk = reader.read(chunk_size)
    bytes_read += chunk_size
    if previous_chunk is not None:
        chunk = previous_chunk + chunk
    try:
        return chunk.decode()
    except UnicodeDecodeError:
        if bytes_read > max_window_size:
            raise UnicodeError(f"Unable to decode frame after reading {bytes_read:,} bytes")
        log.info(f"Decoding error with {bytes_read:,} bytes, reading another chunk")
        return read_and_decode(reader, chunk_size, max_window_size, chunk, bytes_read)


def read_lines_zst(file_name):
    """
    Decodes a zst and returns its lines one by one, along with the progress
    (how many bytes) through the file. Use like this:
        for line, file_bytes_processed in read_lines_zst(sub_file):
            ...
    Borrowed from https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py
    """
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=2**31).stream_reader(file_handle)
        while True:
            chunk = read_and_decode(reader, 2**27, (2**29) * 2)

            if not chunk:
                break
            lines = (buffer + chunk).split("\n")

            for line in lines[:-1]:
                yield line, file_handle.tell()

            buffer = lines[-1]

        reader.close()


def main():
    """
    Example of reading lines from a zst. Borrowed and modified from
        https://github.com/Watchful1/PushshiftDumps/blob/master/scripts/single_file.py
    """
    # Locate the reddit submission and comment files
    base_dir = decouple.config('REDDIT_ARCHIVE_BASE_DIR')

    # Jun 2022 submissions (one of the larger files) to use for testing code
    sub_file = os.path.join(base_dir, 'submissions', 'RS_2022-06.zst')

    file_size = os.stat(sub_file).st_size
    created = None
    bad_lines = 0
    lines_read = 0
    for line, file_bytes_processed in read_lines_zst(sub_file):
        try:
            # Here's where we can do something with the decoded line
            obj = json.loads(line)
            # print(json.dumps(obj, indent=4))
            created = datetime.utcfromtimestamp(int(obj['created_utc']))
        except (KeyError, json.JSONDecodeError):
            bad_lines += 1
        lines_read += 1

        # Log progress through the file
        if lines_read % 10000 == 0:
            log.info(f"{created.strftime('%Y-%m-%d %H:%M:%S')} : {lines_read:,}:"
                     f"{bad_lines:,} : {file_bytes_processed:,}"
                     f":{(file_bytes_processed / file_size) * 100:.0f}%")

    log.info(f"Complete : {lines_read:,} : {bad_lines:,}")


if __name__ == "__main__":
    main()
