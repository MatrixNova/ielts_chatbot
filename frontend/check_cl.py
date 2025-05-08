import sys
import os

print("--- check_cl.py script started ---") # Print immediately

project_root = None
try:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    print(f"Calculated project_root: {project_root}")
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        print(f"Project root ADDED to sys.path.")
    else:
        print(f"Project root was ALREADY in sys.path.")
    print(f"Current sys.path[0]: {sys.path[0] if sys.path else 'EMPTY'}")
    print(f"Full sys.path: {sys.path}")
except Exception as e:
    print(f"CRITICAL ERROR during path setup: {e}")

print("Attempting to import 'chainlit' as cl...")
try:
    import chainlit as cl
    print(f"SUCCESS: Imported 'chainlit' as cl.")
    print(f"cl module object: {cl}")
    print(f"IMPORTANT - cl module location: {cl.__file__}") # WHICH chainlit.py is loaded?

    print("Checking for standard Chainlit decorators...")
    has_on_chat_start = hasattr(cl, 'on_chat_start')
    has_on_message = hasattr(cl, 'on_message')
    has_on_action_clicked = hasattr(cl, 'on_action_clicked')

    print(f"cl has on_chat_start: {has_on_chat_start}")
    print(f"cl has on_message: {has_on_message}")
    print(f"cl has on_action_clicked: {has_on_action_clicked}")

    if not (has_on_chat_start and has_on_message and has_on_action_clicked): # Check all three
        print("ERROR: One or more standard Chainlit decorators are MISSING from the imported 'cl' module!")
    else:
        print("SUCCESS: Standard Chainlit decorators SEEM PRESENT in the 'cl' module.")

except ImportError as e:
    print(f"IMPORT ERROR: Failed to import 'chainlit' as cl. Error: {e}")
except AttributeError as e:
    print(f"ATTRIBUTE ERROR: 'chainlit' module might have imported, but an attribute is missing. Error: {e}")
except Exception as e:
    print(f"UNEXPECTED ERROR during Chainlit import or attribute check: {e}")

print("--- check_cl.py script finished ---")