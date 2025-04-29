import psycopg2
import config
from psycopg2 import pool as psycopg_pool

import logging
logger = logging.getLogger(config.APP_NAME)

db_pool = None

def initialize_pool():
    global db_pool
    if db_pool:
        logger.warning("Database pool already initialized")
        return db_pool
    
    logger.info("Initialize PostgreSQL connection pool")

    try:
        db_pool = psycopg_pool.SimpleConnectionPool(
            minconn = config.POSTGRES_DB_MIN_CONN,
            maxconn= config.POSTGRES_DB_MAX_CONN,
            dbname= config.POSTGRES_DBNAME,
            user= config.POSTGRES_USER,
            password= config.POSTGRES_PASSWORD,
            host= config.POSTGRES_HOST,
            port= config.POSTGRES_PORT
        )

        logger.info("Successfully initialized PostgreSQL connection pool (Min: %d, Max: %d): ", 
                    config.POSTGRES_DB_MIN_CONN, config.POSTGRES_DB_MAX_CONN)

        return db_pool
    
    except (AttributeError, psycopg2.Error) as e:
        logger.exception("Failed to initialized connection pool: %s", e)
        db_pool = None
        return None
    
    except Exception as e:
        logger.exception("Unexpected error during connection pool initialization: %s", e)
        db_pool = None
        return None
    
def close_pool():
    global db_pool
    if db_pool:
        logger.info("Closing Postgre SQL connection pool")

        try:
            db_pool.closeall()
            logger.info("Successfully closed PostgreSQL connection pool")
        
        except Exception as e:
            logger.exception("An exception has occurred when trying to close PostgreSQL connection pool: %s", e)
        
        finally:
            db_pool = None

    else:
        logger.warning("Attempted to close PostgreSQL connection pull, but it was not initialized")

# Context Manager using global db_pool
class db_connection:
    def __enter__(self):
        self.conn = None

        if not db_pool:
            logger.warning("Database pooling is not initialized when entering context. Attemption initialization")
            initialize_pool()

            if not db_pool:
                logger.error("Database pooling still not available after attempt in context manager")
                raise ConnectionError("Database pooling not initialized or failed to initialized")
            
        try:
            self.conn = db_pool.getconn()
            logger.debug("Acquired database connection from pool")
            return self.conn
        
        except Exception as e:
            logger.exception("An exception has occurred when trying to get connection from pool: %s", e)
            self.conn = None
            raise ConnectionError("Failed to get connection from pool") from e
        
    def __exit__(self, exc_type, exc_value, exc_tb):
        if self.conn:
            conn_returned_to_pool = False
            
            try:
                if exc_type:            # If an exception occurs
                    self.conn.rollback()
                    logger.warning("Database transaction rollback due to exception: %s", exc_type)

            except Exception as rb_e:
                logger.exception("An exception has occurred when trying to rollback transaction: %s", rb_e)

            finally:                    # Ensure the connection is returned/closed even when rollback fails
                if db_pool:
                    try:
                        db_pool.putconn(self.conn)
                        logger.debug("Released database connection back to pool")
                        conn_returned_to_pool = True

                    except Exception as e:
                        logger.exception("An exception has occurred when trying to release database connection: %s", e)

                else:
                    logger.warning("Database pool is None during __exit__. Cannot return connection")

                if not db_pool:
                    try:
                        self.conn.close()
                        logger.warning("Force closed connection after failing to return to pool")
                    
                    except Exception as e:
                        logger.exception("Failed to force close connection: %s", e)
        
        return False