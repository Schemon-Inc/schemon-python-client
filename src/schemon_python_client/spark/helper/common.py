def merge_dict(original: dict, update: dict):
    for key, value in update.items():
        # If both original and update have a dictionary under this key, merge them recursively
        if (
            isinstance(value, dict)
            and key in original
            and isinstance(original[key], dict)
        ):
            merge_dict(original[key], value)

        # If both original and update have a list under this key, handle list merging
        elif (
            isinstance(value, list)
            and key in original
            and isinstance(original[key], list)
        ):
            for i, item in enumerate(value):
                if (
                    i < len(original[key])
                    and isinstance(item, dict)
                    and isinstance(original[key][i], dict)
                ):
                    # Recursively merge dictionaries within the list
                    merge_dict(original[key][i], item)
                elif i >= len(original[key]):
                    # Append new items if update list is longer
                    original[key].append(item)

        # Otherwise, just replace the value in original
        else:
            original[key] = value
