# redditdb

Install postgres (or another compatible database) and create a .env with your login info:
```console
DB_HOST=<db address>
DB_PORT=5432
DB_NAME=reddit
DB_USER=<your db user id>
DB_PASSWORD=<your db password>
```
Run
```console
python database.py
```
to create the empty database.
After downloading the reddit torrent from
```console
https://academictorrents.com/details/7c0645c94321311bb05bd879ddee4d0eba08aaee
```
add an entry to the .env file with its location:
```console
REDDIT_ARCHIVE_BASE_DIR=<torrent files location>
```
Run
```console
python router.py
```
to begin parsing the .zst files downloaded in the torrent and inserting the posts/comments into the database. Because the process is slow and CPU-bound, it is recommended to run multiple instances simultaneously. You can run as many processes as you have physical CPU cores.

As the parsing proceeds, it tracks which .zst files it has already processed and writes them to the files
```console
submissions-progress.txt
comments-progress.txt
```
which it will write to the same directory as the reddit torrent files. If the parsing script is interrupted for any reason, you can pick up where you left off in the zst file in progress by deleting the name of that file from the corresponding progress file. For example, if parsing the comments file for 2015-03 is interrupted, you can resume parsing it by running `python router.py` again after first deleting the line RC_2015-03.zst from the `comments-progress.txt` file.
