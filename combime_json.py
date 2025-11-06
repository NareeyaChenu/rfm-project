import json
import os

def combine_json_files(input_folder, output_file):
    combined_data = []

    # Loop through all files in the folder
    for filename in os.listdir(input_folder):
        if filename.endswith(".json"):
            file_path = os.path.join(input_folder, filename)
            with open(file_path, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    # If file contains a list, extend; else append
                    if isinstance(data, list):
                        combined_data.extend(data)
                    else:
                        combined_data.append(data)
                    print(f"✅ Loaded {filename}")
                except json.JSONDecodeError:
                    print(f"⚠️ Skipped invalid JSON file: {filename}")

    # Save combined data
    with open(output_file, 'w', encoding='utf-8') as outfile:
        json.dump(combined_data, outfile, ensure_ascii=False, indent=4)
        print(f"\n🎉 Combined {len(combined_data)} entries into {output_file}")

if __name__ == "__main__":
    input_folder = "./process"   # Folder containing JSON files
    output_file = "all_orders.json"   # Output file name
    combine_json_files(input_folder, output_file)
