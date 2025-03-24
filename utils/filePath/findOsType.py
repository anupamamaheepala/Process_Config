import platform

def find_os_type():
    """
    Determine the operating system type.
    """
    os_type = platform.system()
    return os_type

# Example usage
if __name__ == "__main__":
    os_type = find_os_type()
    print(f"Operating System Type: {os_type}")
