import psycopg2
import threading
import config
from contextlib import contextmanager
from psycopg2 import pool as psycopg_pool

import logging
logger = logging.getLogger(config.APP_NAME)

db_pool = None
pool_init_lock = threading.Lock()

def initialize_pool():
    global db_pool

    with pool_init_lock:
        if db_pool:
            logger.debug("Database pool already initialized")
            return db_pool

        logger.info("Initializing PostgreSQL connection pool")

        try:
            conn_args = {
                "dbname": config.POSTGRES_DBNAME,
                "user": config.POSTGRES_USER,
                "password": config.POSTGRES_PASSWORD,
                "host": config.POSTGRES_HOST,
                "port": config.POSTGRES_PORT
            }

            db_pool = psycopg_pool.SimpleConnectionPool(
                minconn=config.POSTGRES_DB_MIN_CONN,
                maxconn=config.POSTGRES_DB_MAX_CONN,
                **conn_args
            )


            logger.info(
                f"Successfully initialized PostgreSQL pool "
                f"(min: {config.POSTGRES_DB_MIN_CONN}, max: {config.POSTGRES_DB_MAX_CONN})"
            )

            return db_pool

        except Exception as e:
            db_pool = None
            logger.exception("Failed to initialize connection pool")
            raise RuntimeError("PostgreSQL connection pool initialization failed") from e
        
def close_pool():
    global db_pool

    if not db_pool:
        logger.warning("Attempted to close PostgreSQL connection pool, but it was not initialized")
        return

    logger.info("Closing PostgreSQL connection pool")

    try:
        db_pool.closeall()
        logger.info("Successfully closed PostgreSQL connection pool")

    except Exception as e:
        logger.exception(f"Exception while closing PostgreSQL connection pool: {e}")

    finally:
        db_pool = None

def get_pool():
    global db_pool
    if not db_pool:
        logger.debug("Connection pool not initialized, calling initialize_pool()")
        pool = initialize_pool()
        if not pool:
            raise ConnectionError("Database pool could not be initialized")
        return pool
    return db_pool

# Context Manager using global db_pool
@contextmanager
def db_connection():
    conn = None
    pool = get_pool()

    try:
        conn = pool.getconn()
        if not conn:
            raise ConnectionError("Failed to acquire a database connection from the pool")

        logger.debug(f"Acquired connection {id(conn)} from pool")

        yield conn

        conn.commit()
        logger.debug(f"Committed transaction on connection {id(conn)}")

    except Exception as e:
        if conn:
            try:
                conn.rollback()
                logger.warning(f"Rolled back connection {id(conn)} due to an exception")

            except Exception as rollback_err:
                logger.exception(f"Rollback failed on connection {id(conn)}: {rollback_err}")

        logger.exception("Error during DB transaction")
        raise

    finally:
        if conn:
            if db_pool:
                try:
                    pool.putconn(conn)
                    logger.debug(f"Returned connection {id(conn)} to pool")

                except Exception as e:
                    logger.exception(f"Error returning connection to pool: {e}")

                    try:
                        conn.close()
                        logger.warning(f"Force-closed connection {id(conn)} after pool failure")

                    except Exception as e2:
                        logger.exception(f"Failed to force-close connection {id(conn)}: {e2}")

            else:
                logger.warning(
                    f"Pool object (or global db_pool) became None in finally. "
                    f"Attempting to close conn id={id(conn)}."
                )
                try:
                    conn.close()

                except Exception as e_close:
                    logger.exception(f"Failed to force-close connection id={id(conn)} when pool was None: {e_close}")