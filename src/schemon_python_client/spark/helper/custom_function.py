import re


def run_custom_function(raw_input: str, metadata: dict):
    # Step 1: Extract the function string using regex
    # The regex is updated to ensure it stops at the "args:" section
    function_body_match = re.search(
        r"(def\s+[^\n]+\):(?:\n\s+.+)+)(?=\n\s*args:)", raw_input, re.DOTALL
    )

    if function_body_match:
        function_string = function_body_match.group(0)
        function_string = function_string.strip()  # Clean any extra spaces or newlines
    else:
        raise ValueError("No valid function definition found in the input.")

    # Step 2: Extract the args section using a regex for 'args:'
    args_match = re.search(r"args:\s*\n\s*(\w+):\s*(.+)", raw_input)

    if args_match:
        arg_name = args_match.group(1)
        arg_value = args_match.group(2)
        if arg_value == "metadata.full_path":
            arg_value = f'"{metadata["full_path"]}"'
        args = {arg_name: arg_value.strip('"')}
    else:
        raise ValueError("No valid args section found in the input.")

    # Step 4: Execute the function definition
    exec(function_string)

    # Step 5: Call the dynamically defined function
    result = eval(f"custom_function(**{args})")
    return result


def parse_built_in_function(call_string: str):
    # Step 1: Extract the function name
    func_match = re.match(r"(\w+)\(", call_string)
    if func_match:
        func_name = func_match.group(1)
    else:
        raise ValueError("No valid function name found in the input.")

    # Step 2: Extract the arguments
    args_match = re.findall(r"(\w+)=\{?['\"]?([^,'\"\}\)]+)['\"]?\}?", call_string)

    # Step 3: Prepare the args dictionary
    args = {}
    for arg_name, arg_value in args_match:
        # Clean up the string to remove unnecessary characters
        args[arg_name] = arg_value.strip('"')

    return func_name, args
