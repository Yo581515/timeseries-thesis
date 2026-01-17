#!/bin/bash

OUTFILE="tree.txt"
> "$OUTFILE"

# Ignore pattern matching (folders or files)
should_ignore() {
    local path="$1"
    [[ "$path" =~ (^\.git$|^\.git/|/\.git/|^\.idea$|/\.idea/|^venv$|/venv/|^\.vscode$|/\.vscode/|^__pycache__$|/__pycache__/|\.pyc$|\.pyo$|\.DS_Store$|^\.pytest_cache$|/\.pytest_cache/) ]] && return 0
    return 1
}

# Recursively walk the directory and build tree
print_tree() {
    local current_dir="$1"
    local prefix="$2"

    local items=()
    while IFS= read -r -d $'\0' item; do
        # Get relative path
        rel_path="${item#./}"
        should_ignore "$rel_path" && continue
        items+=("$rel_path")
    done < <(find "$current_dir" -mindepth 1 -maxdepth 1 -print0 | sort -z)

    for item in "${items[@]}"; do
        name=$(basename "$item")
        echo "${prefix}├── $name" >> "$OUTFILE"

        if [ -d "$item" ]; then
            print_tree "$item" "${prefix}│   "
        fi
    done
}

print_tree "." ""
echo "✅ Tree saved to $OUTFILE"
