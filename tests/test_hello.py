"""
Tests for the hello world functionality.
"""

import pytest
from actions_package.hello import hello_world, get_greeting_count


class TestHelloWorld:
    """Test cases for hello_world function."""
    
    def test_hello_world_default(self):
        """Test hello_world with default parameter."""
        result = hello_world()
        assert result == "Hello, World!"
    
    def test_hello_world_with_name(self):
        """Test hello_world with custom name."""
        result = hello_world("Python")
        assert result == "Hello, Python!"
    
    def test_hello_world_with_empty_string(self):
        """Test hello_world with empty string."""
        result = hello_world("")
        assert result == "Hello, !"
    
    def test_hello_world_with_special_characters(self):
        """Test hello_world with special characters."""
        result = hello_world("Test-123")
        assert result == "Hello, Test-123!"


class TestGreetingCount:
    """Test cases for get_greeting_count function."""
    
    def test_get_greeting_count_empty_list(self):
        """Test get_greeting_count with empty list."""
        result = get_greeting_count([])
        assert result == 0
    
    def test_get_greeting_count_single_name(self):
        """Test get_greeting_count with single name."""
        result = get_greeting_count(["Alice"])
        assert result == 1
    
    def test_get_greeting_count_multiple_names(self):
        """Test get_greeting_count with multiple names."""
        result = get_greeting_count(["Alice", "Bob", "Charlie"])
        assert result == 3
    
    def test_get_greeting_count_with_duplicates(self):
        """Test get_greeting_count with duplicate names."""
        result = get_greeting_count(["Alice", "Alice", "Bob"])
        assert result == 3


@pytest.mark.parametrize("name,expected", [
    ("Alice", "Hello, Alice!"),
    ("Bob", "Hello, Bob!"),
    ("", "Hello, !"),
    ("123", "Hello, 123!"),
])
def test_hello_world_parametrized(name, expected):
    """Parametrized test for hello_world function."""
    assert hello_world(name) == expected