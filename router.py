import decouple
import os

import parse


def parse_zst_files(directory, progress_list, is_comments):
    """
    Parse the zst-archived reddit comment/submission files found in the given
    directory. Skip any already in the progress list, and add each file to the
    progress list when we start parsing it.

    This is meant to be run by multiple processes simultaneously, since the
    process of decompressing, parsing, and inserting the entries into a
    database is CPU-bound.

    :param directory where to find the zst files
    :param progress_list file listing the already or currently being parsed
    :param is_comments do these files have comments or submissions?
    :return:
    """
    while True:
        # Read the list of files that are already or currently being parsed
        with open(progress_list, 'r') as file:
            finished_or_in_progress = set([line.strip() for line in file])

        # Determine if any files remain to be parsed
        remaining_files = set(os.listdir(directory)) - finished_or_in_progress

        if remaining_files:
            # Choose the next remaining files, alphabetical order
            remaining_files = list(remaining_files)
            remaining_files.sort()
            next_filename = remaining_files[0]
            next_file = os.path.join(directory, next_filename)

            # Note that the file is now in progress
            with open(progress_list, 'a') as f:
                f.write(next_filename + os.linesep)

            # Parse the file and insert it into the database
            print(f"Starting to parse file: {next_file}")
            bad_lines = parse.add_submissions_or_comments(
                next_file, is_comments)
            print(f"... finished parsing file: {next_file}")

            # Log how many bad submissions/comments were found
            with open(progress_list, 'a') as f:
                f.write(f"{next_filename}: {bad_lines}" + os.linesep)

        else:
            # All done. If any files are yet-unparsed, another process is
            # handling them now.
            print(f"No new files to start parsing")
            break


def main():
    # Parse all the comment and submission files, keeping track of which have
    # already been parsed so that multiple processes can work at the same time.

    # Parent directory
    reddit_dir = decouple.config('REDDIT_ARCHIVE_BASE_DIR')

    # Parse submissions
    submission_progress = os.path.join(reddit_dir, 'submissions-progress.txt')
    if not os.path.exists(submission_progress):
        # Create progress file, if necessary
        open(submission_progress, 'w').close()
    submission_dir = os.path.join(reddit_dir, 'submissions')
    parse_zst_files(submission_dir, submission_progress, False)

    # Parse comments
    comment_progress = os.path.join(reddit_dir, 'comments-progress.txt')
    if not os.path.exists(comment_progress):
        # Create progress file, if necessary
        open(comment_progress, 'w').close()
    comment_dir = os.path.join(reddit_dir, 'comments')
    parse_zst_files(comment_dir, comment_progress, True)


if __name__ == '__main__':
    main()
