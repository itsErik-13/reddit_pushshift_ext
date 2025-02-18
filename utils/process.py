import json
import time
from utils import FileHandle
from models import Submission, Comment
from datetime import datetime
from DB import database_connect
import logging
import praw


log = logging.getLogger("log")

reddit = praw.Reddit(
    client_id="QC4LbDNEHL0O_nty-4EjjA",
    client_secret="oofTnK8zmDCqjzlOo8Canim4P8f8zQ",
    user_agent="mentalhealthscrapper"
    )

def process_file(file, queue, field, values, database_name, comment_depth):
    """
    Processes a file by iterating through its lines and writing out the ones where the `field` of the object matches `value`.
    Also passes status information back to the parent via a queue.

    Args:
        file (FileConfig): The file configuration object containing input and output paths.
        queue (Queue): The queue to pass status information back to the parent process.
        field (str): The field of the object to match against the values.
        values (list): The list of values to match against the field.

    Returns:
        None
    """
    
    queue.put(file)
    input_handle = FileHandle(file.input_path)
    
    session = database_connect(database_name)
    
    matched_records = []
    comment_records = []
    batch_size = 500

    value = None
    if len(values) == 1:
        value = min(values)

    try:
        for line, file_bytes_processed in input_handle.yield_lines():
            try:
                obj = json.loads(line)
                matched = False
                observed = obj[field].lower()
                
                if value is not None:
                    if value == observed or value in observed:
                        matched = True

                if matched:
                    matched_records.append(
                            Submission(
                                id=obj.get("id"),
                                author=f"u/{obj.get('author')}" if obj.get("author") else None,
                                title=obj.get("title"),
                                created_utc=datetime.utcfromtimestamp(int(obj.get("created_utc"))) if obj.get("created_utc") else None,
                                selftext=obj.get("selftext", ""),
                                subreddit=obj.get("subreddit"),
                                link_flair_text=obj.get("link_flair_text"),
                                link=f"https://www.reddit.com{obj.get('permalink')}" if obj.get("permalink") else None,
                                num_comments=obj.get("num_comments"),
                                score=obj.get("score"),
                            )
                        )
                    

                    
                    if comment_depth != -1:
                        submission = None
                        for i in range(10):
                            try:
                                submission = reddit.submission(id=obj.get("id"))
                                submission.comments.replace_more(limit=None)
                                break
                            except Exception as e:
                                    log.info(f"Error al obtener comentarios de {obj.get('id')}: {e}")
                                    wait_time = 2   # Espera incremental (10s, 20s, 30s...)
                                    log.info(f"Esperando {wait_time} segundos")
                                    time.sleep(wait_time)
                                    i+=1
                        comments = []
                        if submission:
                            for comment in submission.comments.list():
                                if comment.author and comment.author.name not in ["AutoModerator"] or comment.author is None: #Ignore AutoModerator
                                    comments.append(
                                        Comment(id=comment.id,
                                                post_id=comment.link_id.split('_')[1],
                                                parent_id=comment.parent_id.split('_')[1],
                                                author=f"u/{comment.author.name}" if comment.author else "[deleted]",
                                                created_utc=datetime.utcfromtimestamp(int(comment.created_utc)) if comment.created_utc else None,
                                                body=comment.body,
                                                depth=comment.depth)
                                    )
                            comment_records.extend(comments)
                    file.lines_matched += 1
                if len(matched_records) >= batch_size:
                    session.bulk_save_objects(matched_records)
                    if comment_records:
                        session.bulk_save_objects(comment_records)
                    session.commit()
                    matched_records = []  # Reiniciar lista
                    comment_records = [] # Reiniciar lista
                    log.info(f"DB insert")
                        
            except (KeyError, json.JSONDecodeError, AttributeError) as err:
                file.error_lines += 1
            file.lines_processed += 1
            if file.lines_processed % 1000000 == 0:
                file.bytes_processed = file_bytes_processed
                queue.put(file)

        # Guardar lo que quedó en la lista final
        if matched_records:
            session.bulk_save_objects(matched_records, preserve_order=True)
            session.commit()
        if comment_records:
            session.bulk_save_objects(comment_records, preserve_order=True)
            session.commit()
        session.close()
        file.complete = True
        file.bytes_processed = file.file_size
    except Exception as err:
        file.error_message = str(err)
    queue.put(file)