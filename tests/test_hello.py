import pytest

from tableau_to_looker_parser import hello


def test_hello_with_name():
    """Test hello function with a regular name."""
    result = hello("World")
    assert result == "Hello, World!"


def test_hello_with_empty_string():
    """Test hello function with empty string."""
    result = hello("")
    assert result == "Hello, !"


def test_hello_with_special_characters():
    """Test hello function with special characters."""
    result = hello("Alice & Bob")
    assert result == "Hello, Alice & Bob!"


def test_hello_with_unicode():
    """Test hello function with unicode characters."""
    result = hello("José")
    assert result == "Hello, José!"


def test_hello_with_numbers():
    """Test hello function with numbers."""
    result = hello("User123")
    assert result == "Hello, User123!"


def test_hello_return_type():
    """Test that hello function returns a string."""
    result = hello("Test")
    assert isinstance(result, str)


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Alice", "Hello, Alice!"),
        ("Bob", "Hello, Bob!"),
        ("123", "Hello, 123!"),
        ("Test User", "Hello, Test User!"),
    ],
)
def test_hello_parametrized(name, expected):
    """Test hello function with multiple inputs using parametrize."""
    assert hello(name) == expected
