/* citus--5.2-1--5.2-2.sql */

CREATE FUNCTION master_expire_table_cache(table_name regclass)
	RETURNS VOID
	LANGUAGE C STRICT
	AS 'MODULE_PATHNAME', $$master_expire_table_cache$$;
