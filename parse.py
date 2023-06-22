from datetime import datetime
import json
import os
import time
import zst

import database


def add_submissions_or_comments(zst_file, is_comments):
    """
    Read a zst file of json-formatted reddit submissions or comments and
    add them to the database.

    param: sub_zst An open file reference to a zst file
    param: is_comments Does the file contain comments or submissions (posts)?
    """
    conn = database.new_connection()

    file_size = os.stat(zst_file).st_size
    created = None
    bad_lines = 0
    lines_read = 0
    start_t = time.time()
    for line, file_bytes_processed in zst.read_lines_zst(zst_file):
        try:
            json_data = json.loads(line)
            # print(json.dumps(json_data, indent=4))
            created = datetime.utcfromtimestamp(int(json_data['created_utc']))
            if is_comments:
                database.add_comment(json_data, connection=conn)
            else:
                database.add_submission(json_data, connection=conn)
        except json.JSONDecodeError:
            bad_lines += 1
        except ValueError:
            # This is "A string literal cannot contain NUL (0x00) characters.",
            # probably. Presumably that means there's a problematic character
            # in the reddit post/author/whatever. I don't know how to sanitize
            # these rare entries, so I'm just dropping them.
            bad_lines += 1
        lines_read += 1

        # Log progress through the file
        if lines_read % 10000 == 0:
            elapsed_t = int(time.time() - start_t)
            creation_time = ""
            if created:
                creation_time = created.strftime('%Y-%m-%d %H:%M:%S')
            print(f"{creation_time} :"
                  f"{lines_read:,} read : "
                  f"{bad_lines:,} bad : {file_bytes_processed:,}"
                  f":{(file_bytes_processed / file_size) * 100:.0f}% :"
                  f" Elapsed: {elapsed_t} s")
            conn.commit()

    conn.commit()
    elapsed_t = int(time.time() - start_t)
    print(f"Complete : "
          f"{lines_read:,} read : "
          f"{bad_lines:,} bad : "
          f" Elapsed: {elapsed_t} s")
