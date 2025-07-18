"""
Hello world functionality for the actions package.
"""


def hello_world(name: str = "World") -> str:
    """
    Return a greeting message.
    
    Args:
        name: The name to greet (default: "World")
        
    Returns:
        A greeting message string
        
    Example:
        >>> hello_world()
        'Hello, World!'
        >>> hello_world("Python")
        'Hello, Python!'
    """
    return f"Hello, {name}!"


def get_greeting_count(names: list[str]) -> int:
    """
    Count the number of greetings that would be generated.
    
    Args:
        names: List of names to greet
        
    Returns:
        Number of greetings
        
    Example:
        >>> get_greeting_count(["Alice", "Bob"])
        2
    """
    return len(names)


def main() -> None:
    """Main function for CLI usage."""
    print(hello_world())
    print(hello_world("Python 3.12"))
    print(f"Generated {get_greeting_count(['Alice', 'Bob', 'Charlie'])} greetings!")


if __name__ == "__main__":
    main()