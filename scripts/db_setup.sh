#!/bin/bash

# We execute our JS config files to get the environment variables — this script
# outputs the config as envvars, which we then source into this script's scope.
# It's all to avoid `.env` files. I'm not sure it's worth it 🥹
while IFS= read -r line; do
    eval "$line"
done < <(bun scripts/read_from_config.ts dev)

migration_scripts=$(find ./scripts -name "migration_*.sql")

# Function to execute psql commands
execute_psql() {
    PGPASSWORD=$PG_PASS psql -h $PG_HOST -p $PG_PORT -U $PG_USER -d $PG_OFFLOADER_DB "$@"
}

# Create schema_migrations table
output=$(execute_psql -c "CREATE TABLE IF NOT EXISTS schema_migrations (label VARCHAR(255) PRIMARY KEY);" 2>&1)

# Check if the output contains the "already exists" message
if ! echo "$output" | grep -q "already exists"; then
    echo "Schema migrations table created"
fi

for script in $migration_scripts; do
    # Skip migrations that have already been run
    HAS_MIGRATION_BEEN_RUN=$(execute_psql -t -c "SELECT COUNT(*) FROM schema_migrations WHERE label = '$(basename $script)'")
    if [[ $(echo $HAS_MIGRATION_BEEN_RUN | tr -d ' ') -ne 0 ]]; then
        echo "Skipping migration $script"
        echo $HAS_MIGRATION_BEEN_RUN
        continue
    fi

    echo "Running migration $script"

    execute_psql -f $script

    if [ $? -eq 0 ]; then
        execute_psql -c "INSERT INTO schema_migrations (label) VALUES ('$(basename $script)');"
    fi
done
