filename = "D:\DBA\python\code\Python_test\example.txt"  # Replace with your file name

# Open the file in read mode
with open(filename, "r") as file:
    lines = file.readlines()  # Read all lines into a list

# Modify the line you want to purge
line_to_purge = lines[2].split(":")[0] + ":\n"  # Modify line 3 (index 2)
lines[2] = line_to_purge

# Open the file in write mode to update it
with open(filename, "w") as file:
    file.writelines(lines)
print(lines)
print("File updated successfully.")
