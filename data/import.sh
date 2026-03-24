if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "Error: .env file not found!"
    exit 1
fi

# Set PostgreSQL connection variables
export PGPASSWORD="${POSTGRES_PASSWD}"
PSQL_CMD="psql -h ${POSTGRES_HOST} -p ${POSTGRES_EXT_PORT} -U ${POSTGRES_USER}"

printf "Script starting at %s. \n" "$(date)"
printf "Creating Schema \n"
${PSQL_CMD} -d ${POSTGRES_DBNAME} -c "DROP SCHEMA IF EXISTS ${POSTGRES_SCHEMA} CASCADE;"
${PSQL_CMD} -d ${POSTGRES_DBNAME} -c "CREATE SCHEMA ${POSTGRES_SCHEMA};"

printf "Creating tables in ${POSTGRES_SCHEMA} schema \n"
${PSQL_CMD} -d ${POSTGRES_DBNAME} -c "CREATE TABLE ${POSTGRES_SCHEMA}.name_basics (nconst TEXT PRIMARY KEY, primaryName TEXT, birthYear INTEGER, deathYear INTEGER, primaryProfession TEXT, knownForTitles TEXT);"

printf "Importing name_basics... \n"
${PSQL_CMD} -d ${POSTGRES_DBNAME} <<EOF
\copy ${POSTGRES_SCHEMA}.name_basics FROM '${DATA_DIR}/name.basics.tsv' DELIMITER E'\t' QUOTE E'\b' NULL '\N' CSV HEADER
EOF
if [ $? -ne 0 ]; then echo "Error importing name_basics"; exit 1; fi