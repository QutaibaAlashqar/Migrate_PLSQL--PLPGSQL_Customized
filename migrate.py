import os
import re
import sys



# ----------------------
# Global Paths
# ----------------------



mapping_path = "/PATH/Mapping.txt"  # Mapping File, Can be changed and added more mapping indexes.

input_dir = "/PATH/Functions"     # Input Directory, no matter how much Oracle Files in the Directory.

output_dir = "/PATH/example"      # the Path to create the Directory of the output postgresql.

output_directory_name = "postgresql_migration"      # The Directory name will be created to save all the PLpgSQl in it.

### it will be created a file named as 'migration_log.txt' inside the output_directory_name Directory.



# --------------------------------
# Load mapping file function
# --------------------------------



def load_mapping_file(mapping_path):
    try:
        mapping = {}
        with open(mapping_path, 'r') as file:
            for line in file:
                if line.strip():
                    key, value = line.split('$#||#$')
                    mapping[key.strip()] = value.strip()
        return mapping
    except Exception as e:
        raise Exception(f"Error loading mapping file: {e}")



# --------------------------------
# Function to process the Oracle file and apply mappings
# --------------------------------



def process_file(input_path, output_path, mapping):
    try:
        if not os.path.isfile(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        changes_count = 0
        changes_log = []  # To store details of replacements
        with open(input_path, 'r') as infile, open(output_path, 'w') as outfile:
            for line_number, line in enumerate(infile, start=1):  # Track line number
                original_line = line
                for oracle, postgres in mapping.items():
                    # Perform case-insensitive replacement while preserving original case
                    pattern = rf"\b{re.escape(oracle)}\b"
                    new_line, replacements = re.subn(pattern, postgres, line, flags=re.IGNORECASE)
                    if replacements > 0:
                        changes_log.append((line_number, oracle, postgres, replacements))  # Include line number
                        line = new_line
                        changes_count += replacements
                outfile.write(line)

        return changes_count, changes_log
    except Exception as e:
        raise Exception(f"Error processing file: {e}")



# --------------------------------
# Function to log outputs to a file
# --------------------------------



def log_output(log_path, output):
    with open(log_path, 'a') as log_file:
        log_file.write(output + '\n')



# --------------------------------
# Main function
# --------------------------------



def main():
    try:
        print("--- Oracle to PostgreSQL Migration PLSQL -> PLPGSQL ---\n")

        # Create the output directory for PostgreSQL files
        postgresql_output_dir = os.path.join(output_dir, output_directory_name)
        if not os.path.exists(postgresql_output_dir):
            os.makedirs(postgresql_output_dir)  # Create the output directory if it doesn't exist

        # Prepare log file
        log_file_path = os.path.join(postgresql_output_dir, "migration_log.txt")
        if os.path.exists(log_file_path):
            os.remove(log_file_path)  # Remove existing log file if any

        log_output(log_file_path, "--- Oracle to PostgreSQL Migration PLSQL -> PLPGSQL ---")
        log_output(log_file_path, f"Input Directory: {input_dir}")
        log_output(log_file_path, f"Output Directory: {postgresql_output_dir}")
        log_output(log_file_path, "")

        # Get all .txt files in the input directory
        oracle_files = [f for f in os.listdir(input_dir) if f.endswith('.txt')]

        if not oracle_files:
            raise FileNotFoundError(f"No Oracle .txt files found in directory: {input_dir}")

        log_output(log_file_path, f"Found {len(oracle_files)} Oracle files to migrate:")
        for oracle_file in oracle_files:
            log_output(log_file_path, f"  - {oracle_file}")
        log_output(log_file_path, "\nLoading mapping file...")

        mapping = load_mapping_file(mapping_path)
        log_output(log_file_path, f"Mapping file loaded successfully from: {mapping_path}\n")
        log_output(log_file_path, "----------------------------------------------------------------------")

        total_changes = 0
        converted_files = []  # List to hold the paths of the converted files

        for oracle_file in oracle_files:
            input_path = os.path.join(input_dir, oracle_file)
            output_filename = os.path.splitext(oracle_file)[0] + "_PG.txt"
            output_path = os.path.join(postgresql_output_dir, output_filename)

            log_output(log_file_path, f"\nProcessing {oracle_file}...")
            changes_count, changes_log = process_file(input_path, output_path, mapping)
            total_changes += changes_count

            # Log detailed changes for the current file
            if changes_log:
                log_output(log_file_path, f"Details of changes made in {oracle_file}:")
                for line_number, oracle, postgres, count in changes_log:
                    log_output(log_file_path, f"  Line {line_number}: Replaced {count} occurrence(s) of '{oracle}' with '{postgres}'.")

            converted_files.append(output_path)

        # After processing all files, display the summary
        log_output(log_file_path, "\nAll files have been processed.")
        log_output(log_file_path, "----------------------------------------------------------------------")
        log_output(log_file_path, "Converted Files:")

        for converted_file in converted_files:
            log_output(log_file_path, f"  - {converted_file}")

        log_output(log_file_path, f"\nTotal files processed: {len(oracle_files)}")
        log_output(log_file_path, f"Total changes made: {total_changes}")
        log_output(log_file_path, f"\nThe converted files have been saved to: {postgresql_output_dir}")

        # Also print the same output to the console
        print("\n--- Oracle to PostgreSQL Migration PLSQL -> PLPGSQL ---")
        print(f"Input Directory: {input_dir}")
        print(f"Output Directory: {postgresql_output_dir}")
        print(f"Found {len(oracle_files)} Oracle files to migrate:")
        for oracle_file in oracle_files:
            print(f"  - {oracle_file}")
        print("\nLoading mapping file...")
        print(f"Mapping file loaded successfully from: {mapping_path}\n")
        print("\n----------------------------------------------------------------------")
        
        total_changes = 0
        for oracle_file in oracle_files:
            print(f"\nProcessing {oracle_file}...")
            changes_count, changes_log = process_file(os.path.join(input_dir, oracle_file), os.path.join(postgresql_output_dir, os.path.splitext(oracle_file)[0] + "_PG.txt"), mapping)
            total_changes += changes_count
            
            if changes_log:
                print(f"Details of changes made in {oracle_file}:")
                for line_number, oracle, postgres, count in changes_log:
                    print(f"  Line {line_number}: Replaced {count} occurrence(s) of '{oracle}' with '{postgres}'.")

        print("\nAll files have been processed.")
        print("----------------------------------------------------------------------")
        print("Converted Files:")
        for converted_file in converted_files:
            print(f"  - {converted_file}")

        print(f"\nTotal files processed: {len(oracle_files)}")
        print(f"Total changes made: {total_changes}")
        print(f"\nThe converted files have been saved to: {postgresql_output_dir}")

    except Exception as e:
        print(f"Error: {e}")



# --------------------------------
# Run the script
# --------------------------------



if __name__ == "__main__":
    main()
