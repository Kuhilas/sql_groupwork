import sys
import argparse
import importlib

def get_arguments():
    # Create the argument parser
    parser = argparse.ArgumentParser(description="Run queries from queries.py or queries_2.py")

    # Specify a subcommand for each group of queries
    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Subcommand for queries.py
    parser_queries = subparsers.add_parser('queries', help="Run functions from queries.py")
    parser_queries.add_argument('function', type=str, help="Function name to call")
    parser_queries.add_argument('args', nargs='*', help="Arguments for the function")

    # Subcommand for queries_2.py
    parser_queries_2 = subparsers.add_parser('queries_2', help="Run functions from queries_2.py")
    parser_queries_2.add_argument('function', type=str, help="Function name to call")
    parser_queries_2.add_argument('args', nargs='*', help="Arguments for the function")

    return parser.parse_args()

def main():
    args = get_arguments()

    # Determine the module to import based on the command
    if args.command == 'queries':
        module = importlib.import_module('queries')  # Import queries.py
    elif args.command == 'queries_2':
        module = importlib.import_module('queries_2')  # Import queries_2.py
    else:
        print("Invalid command. Use 'queries' or 'queries_2'.")
        sys.exit(1)

    # Check if the function exists in the selected module
    if hasattr(module, args.function):
        func = getattr(module, args.function)
        try:
            # Call the function with the provided arguments
            func(*args.args)
        except TypeError as e:
            print(f"Error: {e}")
            print("Ensure the number and types of arguments match the function definition.")
    else:
        print(f"Function '{args.function}' not found in {args.command}.py")
        sys.exit(1)

if __name__ == '__main__':
    main()