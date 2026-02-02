import argparse
import importlib
import sys
import json
import os
from dotenv import load_dotenv
from tethys_tasks.base import BaseTask
import tethys_tasks

# Load environment variables from .env if it exists
load_dotenv()

def main():
    parser = argparse.ArgumentParser(description='Tethys Task Runner')
    parser.add_argument('class_name', type=str, help='Name of the class to use (e.g., ERA5_ZAMBEZI_T2M)')
    parser.add_argument('function_name', type=str, help='Name of the function to use (e.g., retrieve)')
    parser.add_argument('--class_args', type=str, default='[]', help='JSON string with arguments to initialize the class')
    parser.add_argument('--class_kwargs', type=str, default='{}', help='JSON string with kw_arguments to initialize the class')
    parser.add_argument('--fun_args', type=str, default='[]', help='JSON string with arguments to the function')
    parser.add_argument('--fun_kwargs', type=str, default='{}', help='JSON string with kw_arguments to the function')

    args = parser.parse_args()

    # Get the class
    try:
        print(tethys_tasks.__all__)
        print(args.class_name)
        cls = getattr(tethys_tasks, args.class_name, None)
        if cls is None:
            raise Exception(f'Class {args.class_name} not found.')
    except Exception as ex:
        print(ex)
        sys.exit(1)

    # Parse arguments
    try:
        c_args = json.loads(args.class_args)
        c_kwargs = json.loads(args.class_kwargs)
        f_args = json.loads(args.fun_args)
        f_kwargs = json.loads(args.fun_kwargs)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON arguments: {e}")
        sys.exit(1)

    # Instantiate the class
    try:
        task_instance = cls(*c_args, **c_kwargs)
    except Exception as e:
        print(f"Error instantiating class {args.class_name}: {e}")
        sys.exit(1)
    
    # Get the function
    try:
        func = getattr(task_instance, args.function_name)
    except AttributeError:
        print(f"Function {args.function_name} not found in class {args.class_name}")
        sys.exit(1)

    # Run the function
    print(f"Running {args.class_name}.{args.function_name}...")
    try:
        result = func(*f_args, **f_kwargs)
        if result is not None:
            print(f"Result: {result}")
    except Exception as e:
        raise(e)
        print(f"Error running function {args.function_name}: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
