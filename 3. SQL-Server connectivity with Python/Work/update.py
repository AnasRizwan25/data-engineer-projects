from connect import create_connection
import logging

def update_author_name(author_id: int, first_name: str) -> bool:
    conn = create_connection()
    if conn is None:
        return False

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE Authors SET FirstName = ? WHERE AuthorID = ?",
                (first_name, author_id),
            )
            conn.commit()
            
            logging.info(f'{cursor.rowcount} rows updated successfully.')
            
            return cursor.rowcount == 1
    except Exception as e:
        logging.error(f"Error updating customer email: {e}")
        return False

logging.basicConfig(level=logging.INFO)
author_id = 1
new_first_name = 'zohaib'
result = update_author_name(author_id, new_first_name)
if result:
    print("Author name updated successfully.")
else:
    print("Failed to update author name.")
