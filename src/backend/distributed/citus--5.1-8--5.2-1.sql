/* citus--5.1-8--5.2-1.sql */

/* empty, but required to update the extension version */
CREATE FUNCTION master_create_colocated_table(source_relation_name text, dest_relation_name text)
    RETURNS VOID
    LANGUAGE C STABLE STRICT
    AS 'MODULE_PATHNAME', $$master_create_colocated_table$$;
COMMENT ON FUNCTION master_create_colocated_table(relation_name text,  dest_relation_name text)
    IS 'TODO: update comment';
