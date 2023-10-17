#!/bin/bash
echo -n "Supply a full (or relative to current) directory path where 'hotel-weather' directory is located: "
read HW_PARENT_PATH
if [[ $HW_PARENT_PATH ]]; then
    python ./incremental_copy.py "$HW_PARENT_PATH"
else 
    echo "You didn't write anything, but this is required!"
fi
