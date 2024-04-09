#!/bin/bash
skip=$((1 + $RANDOM % 10))
echo "Skip: $Skip"
Counter=0
while IFS= read -r LinefromFile || [[ -n "${LinefromFile}" ]]; do
    ((Counter++))
    remainder=$((Counter % skip))
    if [ "$remainder" -eq 0 ]; then
        echo "copying file: $Counter"
        xrdcp -f $LinefromFile /dev/null
    fi
    echo "Accessing line $Counter: ${LinefromFile}"
done < $1