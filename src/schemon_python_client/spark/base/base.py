class Base:
    def to_dict(self):
        def serialize(value):
            """
            Helper function to recursively serialize objects, lists, and dictionaries.
            """
            if value is None:
                return None  # If the value is None, return None
            elif isinstance(value, Base):
                return (
                    value.to_dict()
                )  # Recursively convert objects with a to_dict method
            elif isinstance(value, list):
                return [
                    serialize(item) for item in value
                ]  # Serialize each item in the list
            elif isinstance(value, dict):
                return {
                    k: serialize(v) for k, v in value.items()
                }  # Serialize dictionary values
            else:
                return value  # Return the value as-is if it's a primitive type

        # Serialize the instance's attributes, including nested structures
        return {
            k: serialize(v)
            for k, v in self.__dict__.items()
            if not k.startswith("_sa_")
        }
