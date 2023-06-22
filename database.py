import decouple
import json
import psycopg2


def new_connection():
    print('getting new connection')

    # Set up db variables
    db_host = decouple.config('DB_HOST')
    db_port = decouple.config('DB_PORT', default=5432, cast=int)
    db_name = decouple.config('DB_NAME')
    db_user = decouple.config('DB_USER')
    db_password = decouple.config('DB_PASSWORD')

    # connect to the database
    conn = psycopg2.connect(
        host=db_host,
        port=db_port,
        dbname=db_name,
        user=db_user,
        password=db_password
    )
    return conn


def create_database(connection=None):
    conn = connection if connection else new_connection()
    cur = conn.cursor()

    # Create the new submissions (posts) table
    # year_month: e.g., 202203
    cur.execute("""
        CREATE TABLE submission (
            id BIGSERIAL PRIMARY KEY,
            archived BOOLEAN,
            author VARCHAR(255),
            created_utc INTEGER,
            name VARCHAR(255) UNIQUE,
            reddit_id VARCHAR(255) UNIQUE,
            selftext TEXT,
            subreddit VARCHAR(255),
            title TEXT,
            url TEXT
        );
    """)

    # Create the new comments table
    # year_month: e.g., 202203
    cur.execute("""
        CREATE TABLE comment (
            id BIGSERIAL PRIMARY KEY,
            archived BOOLEAN,
            author VARCHAR(255),
            body TEXT,
            created_utc INTEGER,
            parent_id VARCHAR(255),
            reddit_id VARCHAR(255) UNIQUE,
            subreddit VARCHAR(255)
        );
    """)


def add_submission(submission, connection=None):
    """
    Add a reddit submission (post) to the database. Does not perform a commit.

    Args:
        submission: submission data as a dict
        connection: database connection to use
    """
    conn = connection if connection else new_connection()
    cur = conn.cursor()

    # Account for fields that are sometimes missing
    if 'name' not in submission:
        submission['name'] = None
    if 'subreddit' not in submission:
        submission['subreddit'] = None
    if 'archived' not in submission:
        submission['archived'] = None

    # Adjust variable names to match our schema
    submission['reddit_id'] = submission['id']

    # Check if the reddit_id already exists
    cur.execute("SELECT 1 FROM submission WHERE reddit_id = %(reddit_id)s",
                submission)
    exists = cur.fetchone()
    if exists:
        return

    # Convert the JSON data to a string
    json_str = json.dumps(submission)

    # SQL INSERT statement
    insert_query = """
        INSERT INTO submission (
            archived,
            author,
            created_utc,
            name,
            reddit_id,
            selftext,
            subreddit,
            title,
            url
        ) VALUES (
            %(archived)s,
            %(author)s,
            %(created_utc)s,
            %(name)s,
            %(reddit_id)s,
            %(selftext)s,
            %(subreddit)s,
            %(title)s,
            %(url)s
        ) 
    """

    # Execute the INSERT statement with the JSON data
    cur.execute(insert_query, json.loads(json_str))


def add_comment(comment, connection=None):
    """
    Add a reddit comment to the database. Doesn't perform a commit

    Args:
        comment: comment data as a dict
        connection: database connection to use
    """
    conn = connection if connection else new_connection()
    cur = conn.cursor()

    # Adjust variable names to match our schema
    comment['reddit_id'] = comment['id']

    # Account for fields that are sometimes missing
    if 'archived' not in comment:
        comment['archived'] = None
    if 'subreddit' not in comment:
        comment['subreddit'] = None

    # Convert the JSON data to a string
    json_str = json.dumps(comment)

    # Check if the reddit_id already exists
    cur.execute("SELECT 1 FROM comment WHERE reddit_id = %(reddit_id)s",
                comment)
    exists = cur.fetchone()
    if exists:
        return

    # SQL INSERT statement
    insert_query = """
        INSERT INTO comment (
            archived,
            author,
            body,
            created_utc,
            reddit_id,
            parent_id,
            subreddit
        ) VALUES (
            %(archived)s,
            %(author)s,
            %(body)s,
            %(created_utc)s,
            %(reddit_id)s,
            %(parent_id)s,
            %(subreddit)s
        )
    """

    # Execute the INSERT statement with the JSON data
    cur.execute(insert_query, json.loads(json_str))


def main():
    # Create the database
    con = new_connection()
    create_database(con)
    con.commit()


if __name__ == '__main__':
    main()
