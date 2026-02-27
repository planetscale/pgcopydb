/*
 * src/bin/pgcopydb/filtering.c
 *     Implementation of a CLI which lets you run individual routines
 *     directly
 */

#include <errno.h>
#include <getopt.h>
#include <inttypes.h>

#include "parson.h"

#include "env_utils.h"
#include "file_utils.h"
#include "ini.h"
#include "log.h"
#include "filtering.h"
#include "parsing_utils.h"
#include "string_utils.h"


static bool parse_filter_quoted_table_name(SourceFilterTable *table,
										   const char *qname);


/*
 * filterTypeToString returns a string reprensentation of the enum value.
 */
char *
filterTypeToString(SourceFilterType type)
{
	switch (type)
	{
		case SOURCE_FILTER_TYPE_NONE:
		{
			return "SOURCE_FILTER_TYPE_NONE";
		}

		case SOURCE_FILTER_TYPE_INCL:
		{
			return "SOURCE_FILTER_TYPE_INCL";
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			return "SOURCE_FILTER_TYPE_EXCL";
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			return "SOURCE_FILTER_TYPE_LIST_NOT_INCL";
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			return "SOURCE_FILTER_LIST_EXCL";
		}

		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			return "SOURCE_FILTER_TYPE_EXCL_INDEX";
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return "SOURCE_FILTER_TYPE_LIST_EXCL_INDEX";
		}

		case SOURCE_FILTER_TYPE_EXCL_EXTENSION:
		{
			return "SOURCE_FILTER_TYPE_EXCL_EXTENSION";
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_EXTENSION:
		{
			return "SOURCE_FILTER_TYPE_LIST_EXCL_EXTENSION";
		}
	}

	/* that's a bug, the lack of a default branch above should prevent it */
	return "SOURCE FILTER TYPE UNKNOWN";
}


/*
 * filterTypeComplement returns the complement to the given filtering type:
 * instead of listing the include-only tables, list the tables that are not
 * included; instead of listing tables that are not excluded, list the tables
 * that are excluded.
 */
SourceFilterType
filterTypeComplement(SourceFilterType type)
{
	switch (type)
	{
		case SOURCE_FILTER_TYPE_INCL:
		{
			return SOURCE_FILTER_TYPE_LIST_NOT_INCL;
		}

		case SOURCE_FILTER_TYPE_LIST_NOT_INCL:
		{
			return SOURCE_FILTER_TYPE_INCL;
		}

		case SOURCE_FILTER_TYPE_EXCL:
		{
			return SOURCE_FILTER_TYPE_LIST_EXCL;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL:
		{
			return SOURCE_FILTER_TYPE_EXCL;
		}

		case SOURCE_FILTER_TYPE_EXCL_INDEX:
		{
			return SOURCE_FILTER_TYPE_LIST_EXCL_INDEX;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_INDEX:
		{
			return SOURCE_FILTER_TYPE_EXCL_INDEX;
		}

		case SOURCE_FILTER_TYPE_EXCL_EXTENSION:
		{
			return SOURCE_FILTER_TYPE_LIST_EXCL_EXTENSION;
		}

		case SOURCE_FILTER_TYPE_LIST_EXCL_EXTENSION:
		{
			return SOURCE_FILTER_TYPE_EXCL_EXTENSION;
		}

		default:
		{
			return SOURCE_FILTER_TYPE_NONE;
		}
	}
}


/*
 * parse_filters
 */
bool
parse_filters(const char *filename, SourceFilters *filters)
{
	char *fileContents = NULL;
	long fileSize = 0L;

	/* read the current postgresql.conf contents */
	if (!read_file(filename, &fileContents, &fileSize))
	{
		return false;
	}

	ini_t *ini = ini_load(fileContents, NULL);

	/*
	 * The index in the sections array matches the SourceFilterSection enum
	 * values.
	 */
	struct section
	{
		char name[NAMEDATALEN];
		SourceFilterSection section;
		SourceFilterTableList *list;
	}
	sections[] =
	{
		{ "include-only-schema", SOURCE_FILTER_INCLUDE_ONLY_SCHEMA, NULL },
		{ "exclude-schema", SOURCE_FILTER_EXCLUDE_SCHEMA, NULL },
		{
			"exclude-table",
			SOURCE_FILTER_EXCLUDE_TABLE,
			&(filters->excludeTableList)
		},
		{
			"exclude-table-data",
			SOURCE_FILTER_EXCLUDE_TABLE_DATA,
			&(filters->excludeTableDataList)
		},
		{
			"exclude-index",
			SOURCE_FILTER_EXCLUDE_INDEX,
			&(filters->excludeIndexList)
		},
		{
			"include-only-table",
			SOURCE_FILTER_INCLUDE_ONLY_TABLE,
			&(filters->includeOnlyTableList)
		},
		{ "exclude-extension", SOURCE_FILTER_EXCLUDE_EXTENSION, NULL },
		{ "include-only-extension", SOURCE_FILTER_INCLUDE_ONLY_EXTENSION, NULL },
		{ "", SOURCE_FILTER_UNKNOWN, NULL },
	};

	for (int i = 0; sections[i].name[0] != '\0'; i++)
	{
		char *sectionName = sections[i].name;

		int sectionIndex = ini_find_section(ini, sectionName, 0);

		if (sectionIndex == INI_NOT_FOUND)
		{
			log_debug("Sections \"%s\" not found", sectionName);
			continue;
		}

		if (strcmp(ini_section_name(ini, sectionIndex), sectionName) != 0)
		{
			/* skip prefix match, only accept full length match */
			continue;
		}

		int optionCount = ini_property_count(ini, sectionIndex);

		log_debug("Section \"%s\" has %d entries", sections[i].name, optionCount);

		if (optionCount <= 0)
		{
			continue;
		}

		/*
		 * The index in the sections table is a SourceFilterSection enum value.
		 */
		switch (sections[i].section)
		{
			case SOURCE_FILTER_INCLUDE_ONLY_SCHEMA:
			{
				filters->includeOnlySchemaList.count = optionCount;
				filters->includeOnlySchemaList.array =
					(SourceFilterSchema *) calloc(optionCount,
												  sizeof(SourceFilterSchema));

				if (filters->includeOnlySchemaList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterSchema *schema =
						&(filters->includeOnlySchemaList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(schema->nspname, optionName, sizeof(schema->nspname));

					log_debug("including only schema \"%s\"", schema->nspname);
				}
				break;
			}

			case SOURCE_FILTER_EXCLUDE_SCHEMA:
			{
				filters->excludeSchemaList.count = optionCount;
				filters->excludeSchemaList.array =
					(SourceFilterSchema *) calloc(optionCount,
												  sizeof(SourceFilterSchema));

				if (filters->excludeSchemaList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterSchema *schema =
						&(filters->excludeSchemaList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(schema->nspname, optionName, sizeof(schema->nspname));

					log_debug("excluding schema \"%s\"", schema->nspname);
				}
				break;
			}

			case SOURCE_FILTER_EXCLUDE_TABLE:
			case SOURCE_FILTER_EXCLUDE_TABLE_DATA:
			case SOURCE_FILTER_EXCLUDE_INDEX:
			case SOURCE_FILTER_INCLUDE_ONLY_TABLE:
			{
				SourceFilterTableList *list = sections[i].list;

				list->count = optionCount;
				list->array =
					(SourceFilterTable *) calloc(optionCount,
												 sizeof(SourceFilterTable));

				if (list->array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterTable *table = &(list->array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					if (!parse_filter_quoted_table_name(table, optionName))
					{
						/* errors have already been logged */
						(void) ini_destroy(ini);
						return false;
					}

					log_trace("%s \"%s\".\"%s\"",
							  sections[i].name,
							  table->nspname,
							  table->relname);
				}

				break;
			}

			case SOURCE_FILTER_INCLUDE_ONLY_EXTENSION:
			{
				filters->includeOnlyExtensionList.count = optionCount;
				filters->includeOnlyExtensionList.array =
					(SourceFilterExtension *) calloc(optionCount,
													 sizeof(SourceFilterExtension));

				if (filters->includeOnlyExtensionList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterExtension *extension =
						&(filters->includeOnlyExtensionList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(extension->extname, optionName, sizeof(extension->extname));

					log_debug("including only extension \"%s\"", extension->extname);
				}
				break;
			}

			case SOURCE_FILTER_EXCLUDE_EXTENSION:
			{
				filters->excludeExtensionList.count = optionCount;
				filters->excludeExtensionList.array =
					(SourceFilterExtension *) calloc(optionCount,
													 sizeof(SourceFilterExtension));

				if (filters->excludeExtensionList.array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					return false;
				}

				for (int o = 0; o < optionCount; o++)
				{
					SourceFilterExtension *extension =
						&(filters->excludeExtensionList.array[o]);

					const char *optionName =
						ini_property_name(ini, sectionIndex, o);

					strlcpy(extension->extname, optionName, sizeof(extension->extname));

					log_debug("excluding extension \"%s\"", extension->extname);
				}
				break;
			}

			default:
			{
				log_error("BUG: unknown section number %d", i);
				(void) ini_destroy(ini);
				return false;
			}
		}
	}

	(void) ini_destroy(ini);

	/*
	 * Now implement some checks: we can't implement both include-only-table
	 * and any other filtering rule, which are exclusion rules. Otherwise it's
	 * unclear what to do with tables that are not excluded and not included
	 * either.
	 *
	 * Using both exclude-schema and include-only-table sections is allowed,
	 * the user needs to pay attention not to exclude schemas of tables that
	 * are then to be included only.
	 *
	 * Using both exclude-schema and include-only-schema is disallowed too. It
	 * does not make sense to use both at the same time.
	 */
	if (filters->includeOnlySchemaList.count > 0 &&
		filters->excludeSchemaList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains %d entries "
				  "in section \"%s\" and %d entries in section \"%s\", "
				  "please use only one of these section.",
				  filename,
				  filters->includeOnlySchemaList.count,
				  "include-only-schema",
				  filters->excludeSchemaList.count,
				  "exclude-schema");
		return false;
	}

	if (filters->includeOnlyTableList.count > 0 &&
		filters->excludeTableList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains "
				  "%d entries in section \"%s\" and %d entries in "
				  "section \"%s\", please use only one of these sections.",
				  filename,
				  filters->includeOnlyTableList.count,
				  "include-only-table",
				  filters->excludeTableList.count,
				  "exclude-table");
		return false;
	}

	if (filters->includeOnlyTableList.count > 0 &&
		filters->excludeSchemaList.count > 0)
	{
		log_warn("Filtering setup in \"%s\" contains %d entries "
				 "in \"%s\" section and %d entries in \"%s\" section, "
				 "please make sure not to filter-out schema of "
				 "tables you want to include",
				 filename,
				 filters->includeOnlyTableList.count,
				 "include-only-table",
				 filters->excludeSchemaList.count,
				 "exclude-schema");
	}

	if (filters->includeOnlyExtensionList.count > 0 &&
		filters->excludeExtensionList.count > 0)
	{
		log_error("Filtering setup in \"%s\" contains %d entries "
				  "in section \"%s\" and %d entries in section \"%s\", "
				  "please use only one of these sections.",
				  filename,
				  filters->includeOnlyExtensionList.count,
				  "include-only-extension",
				  filters->excludeExtensionList.count,
				  "exclude-extension");
		return false;
	}

	/*
	 * Now assign a proper type to the source filter.
	 */
	if (filters->includeOnlyTableList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_INCL;
	}

	/*
	 * include-only-schema works the same as an exclude-schema filter, it only
	 * allows another spelling of it that might be more useful -- it's still an
	 * exclusion filter.
	 */
	else if (filters->includeOnlySchemaList.count > 0 ||
			 filters->excludeSchemaList.count > 0 ||
			 filters->excludeTableList.count > 0 ||
			 filters->excludeTableDataList.count > 0 ||
			 filters->excludeExtensionList.count > 0 ||
			 filters->includeOnlyExtensionList.count > 0)
	{
		filters->type = SOURCE_FILTER_TYPE_EXCL;
	}
	else if (filters->excludeIndexList.count > 0)
	{
		/*
		 * If we reach this part of the code, it means we didn't include-only
		 * tables nor exclude any table (exclude-schema, exclude-table,
		 * exclude-table-data have not been used in the filtering setup), still
		 * the exclude-index clause has been used.
		 */
		filters->type = SOURCE_FILTER_TYPE_EXCL_INDEX;
	}
	else
	{
		filters->type = SOURCE_FILTER_TYPE_NONE;
	}

	return true;
}


/*
 * parse_filter_quoted_table_name parses a maybe-quoted qualified relation name
 * (schemaname.relname) into a pre-alllocated SourceFilterTable.
 */
static bool
parse_filter_quoted_table_name(SourceFilterTable *table, const char *qname)
{
	if (qname == NULL || qname[0] == '\0')
	{
		log_error("Failed to parse empty qualified name");
		return false;
	}

	char *dot = strchr(qname, '.');

	if (dot == NULL)
	{
		log_error("Failed to find a dot separator in qualified name \"%s\"",
				  qname);
		return false;
	}
	else if (dot == qname)
	{
		log_error("Failed to parse qualified name \"%s\": it starts with a dot",
				  qname);
		return false;
	}

	if (qname[0] == '"' && *(dot - 1) != '"')
	{
		log_error("Failed to parse quoted relation name: \"%s\"", qname);
		return false;
	}

	char *nspnameStart = qname[0] == '"' ? (char *) qname + 1 : (char *) qname;
	char *nspnameEnd = *(dot - 1) == '"' ? dot - 1 : dot;

	/* skip last character of the range, either a closing quote or the dot */
	int nsplen = nspnameEnd - nspnameStart;

	size_t nspbytes =
		sformat(table->nspname, sizeof(table->nspname), "%.*s",
				nsplen,
				nspnameStart);

	if (nspbytes >= sizeof(table->nspname))
	{
		log_error("Failed to parse schema name \"%s\" (%d bytes long), "
				  "pgcopydb and Postgres only support names up to %zu bytes",
				  table->nspname,
				  nsplen,
				  sizeof(table->nspname));
		return false;
	}

	if (strcmp(dot, ".") == 0)
	{
		log_error("Failed to parse empty relation name after the dot in \"%s\"",
				  qname);
		return false;
	}

	char *ptr = dot + 1;
	char *end = strchr(ptr, '\0');

	if (ptr[0] == '"' && *(end - 1) != '"')
	{
		log_error("Failed to parse quoted relation name: \"%s\"", ptr);
		return false;
	}

	char *relnameStart = ptr[0] == '"' ? ptr + 1 : ptr;
	char *relnameEnd = *(end - 1) == '"' ? end - 1 : end;
	int rellen = relnameEnd - relnameStart + 1;

	size_t relbytes =
		sformat(table->relname, sizeof(table->relname), "%.*s",
				rellen,
				relnameStart);

	if (relbytes >= sizeof(table->relname))
	{
		log_error("Failed to parse relation name \"%s\" (%d bytes long), "
				  "pgcopydb and Postgres only support names up to %zu bytes",
				  table->relname,
				  rellen,
				  sizeof(table->relname));
		return false;
	}

	return true;
}


/*
 * copydb_filtering_as_json prepares the filtering setup of the CopyDataSpecs
 * as a JSON object within the given JSON_Value.
 */
bool
filters_as_json(SourceFilters *filters, JSON_Value *jsFilter)
{
	JSON_Object *jsFilterObj = json_value_get_object(jsFilter);

	json_object_set_string(jsFilterObj,
						   "type",
						   filterTypeToString(filters->type));

	/* include-only-schema */
	if (filters->includeOnlySchemaList.count > 0)
	{
		JSON_Value *jsSchema = json_value_init_array();
		JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

		for (int i = 0; i < filters->includeOnlySchemaList.count; i++)
		{
			char *nspname = filters->includeOnlySchemaList.array[i].nspname;

			json_array_append_string(jsSchemaArray, nspname);
		}

		json_object_set_value(jsFilterObj, "include-only-schema", jsSchema);
	}

	/* exclude-schema */
	if (filters->excludeSchemaList.count > 0)
	{
		JSON_Value *jsSchema = json_value_init_array();
		JSON_Array *jsSchemaArray = json_value_get_array(jsSchema);

		for (int i = 0; i < filters->excludeSchemaList.count; i++)
		{
			char *nspname = filters->excludeSchemaList.array[i].nspname;

			json_array_append_string(jsSchemaArray, nspname);
		}

		json_object_set_value(jsFilterObj, "exclude-schema", jsSchema);
	}

	/* include-only-extension */
	if (filters->includeOnlyExtensionList.count > 0)
	{
		JSON_Value *jsExt = json_value_init_array();
		JSON_Array *jsExtArray = json_value_get_array(jsExt);

		for (int i = 0; i < filters->includeOnlyExtensionList.count; i++)
		{
			char *extname = filters->includeOnlyExtensionList.array[i].extname;

			json_array_append_string(jsExtArray, extname);
		}

		json_object_set_value(jsFilterObj, "include-only-extension", jsExt);
	}

	/* exclude-extension */
	if (filters->excludeExtensionList.count > 0)
	{
		JSON_Value *jsExt = json_value_init_array();
		JSON_Array *jsExtArray = json_value_get_array(jsExt);

		for (int i = 0; i < filters->excludeExtensionList.count; i++)
		{
			char *extname = filters->excludeExtensionList.array[i].extname;

			json_array_append_string(jsExtArray, extname);
		}

		json_object_set_value(jsFilterObj, "exclude-extension", jsExt);
	}

	/* exclude table lists */
	struct section
	{
		char name[PG_NAMEDATALEN];
		SourceFilterTableList *list;
	};

	struct section sections[] = {
		{ "exclude-table", &(filters->excludeTableList) },
		{ "exclude-table-data", &(filters->excludeTableDataList) },
		{ "exclude-index", &(filters->excludeIndexList) },
		{ "include-only-table", &(filters->includeOnlyTableList) },
		{ "", NULL },
	};

	for (int i = 0; sections[i].list != NULL; i++)
	{
		char *sectionName = sections[i].name;
		SourceFilterTableList *list = sections[i].list;

		if (list->count > 0)
		{
			JSON_Value *jsList = json_value_init_array();
			JSON_Array *jsListArray = json_value_get_array(jsList);

			for (int j = 0; j < list->count; j++)
			{
				SourceFilterTable *table = &(list->array[j]);

				JSON_Value *jsTable = json_value_init_object();
				JSON_Object *jsTableObj = json_value_get_object(jsTable);

				json_object_set_string(jsTableObj, "schema", table->nspname);
				json_object_set_string(jsTableObj, "name", table->relname);

				json_array_append_value(jsListArray, jsTable);
			}

			json_object_set_value(jsFilterObj, sectionName, jsList);
		}
	}

	return true;
}


/*
 * filters_from_json parses a JSON representation of filters and populates a
 * SourceFilters structure. This is the inverse of filters_as_json().
 */
bool
filters_from_json(const char *jsonString, SourceFilters *filters)
{
	if (jsonString == NULL || filters == NULL)
	{
		log_error("BUG: filters_from_json called with NULL argument");
		return false;
	}

	/* Initialize filters to empty state */
	filters->prepared = false;
	filters->isReadOnly = false;
	filters->type = SOURCE_FILTER_TYPE_NONE;
	filters->includeOnlySchemaList.count = 0;
	filters->includeOnlySchemaList.array = NULL;
	filters->excludeSchemaList.count = 0;
	filters->excludeSchemaList.array = NULL;
	filters->includeOnlyTableList.count = 0;
	filters->includeOnlyTableList.array = NULL;
	filters->excludeTableList.count = 0;
	filters->excludeTableList.array = NULL;
	filters->excludeTableDataList.count = 0;
	filters->excludeTableDataList.array = NULL;
	filters->excludeIndexList.count = 0;
	filters->excludeIndexList.array = NULL;
	filters->includeOnlyExtensionList.count = 0;
	filters->includeOnlyExtensionList.array = NULL;
	filters->excludeExtensionList.count = 0;
	filters->excludeExtensionList.array = NULL;
	filters->ctePreamble = NULL;

	/* Parse JSON string */
	JSON_Value *jsFilter = json_parse_string(jsonString);

	if (jsFilter == NULL)
	{
		log_error("Failed to parse filters JSON: %s", jsonString);
		return false;
	}

	JSON_Object *jsFilterObj = json_value_get_object(jsFilter);

	if (jsFilterObj == NULL)
	{
		log_error("Filters JSON is not an object: %s", jsonString);
		json_value_free(jsFilter);
		return false;
	}

	/* Parse type field */
	const char *typeStr = json_object_get_string(jsFilterObj, "type");

	if (typeStr != NULL)
	{
		if (strcmp(typeStr, "SOURCE_FILTER_TYPE_NONE") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_NONE;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_TYPE_INCL") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_INCL;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_TYPE_EXCL") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_EXCL;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_TYPE_LIST_NOT_INCL") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_LIST_NOT_INCL;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_LIST_EXCL") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_LIST_EXCL;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_TYPE_EXCL_INDEX") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_EXCL_INDEX;
		}
		else if (strcmp(typeStr, "SOURCE_FILTER_TYPE_LIST_EXCL_INDEX") == 0)
		{
			filters->type = SOURCE_FILTER_TYPE_LIST_EXCL_INDEX;
		}
		else
		{
			log_warn("Unknown filter type in JSON: %s", typeStr);
		}
	}

	/* Parse include-only-schema array */
	JSON_Array *includeSchemaArray =
		json_object_get_array(jsFilterObj, "include-only-schema");

	if (includeSchemaArray != NULL)
	{
		size_t count = json_array_get_count(includeSchemaArray);
		filters->includeOnlySchemaList.count = count;

		if (count > 0)
		{
			filters->includeOnlySchemaList.array =
				(SourceFilterSchema *) calloc(count, sizeof(SourceFilterSchema));

			if (filters->includeOnlySchemaList.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(jsFilter);
				return false;
			}

			for (size_t i = 0; i < count; i++)
			{
				const char *nspname = json_array_get_string(includeSchemaArray, i);

				if (nspname != NULL)
				{
					strlcpy(filters->includeOnlySchemaList.array[i].nspname,
							nspname,
							PG_NAMEDATALEN);
				}
			}
		}
	}

	/* Parse exclude-schema array */
	JSON_Array *excludeSchemaArray =
		json_object_get_array(jsFilterObj, "exclude-schema");

	if (excludeSchemaArray != NULL)
	{
		size_t count = json_array_get_count(excludeSchemaArray);
		filters->excludeSchemaList.count = count;

		if (count > 0)
		{
			filters->excludeSchemaList.array =
				(SourceFilterSchema *) calloc(count, sizeof(SourceFilterSchema));

			if (filters->excludeSchemaList.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(jsFilter);
				return false;
			}

			for (size_t i = 0; i < count; i++)
			{
				const char *nspname = json_array_get_string(excludeSchemaArray, i);

				if (nspname != NULL)
				{
					strlcpy(filters->excludeSchemaList.array[i].nspname,
							nspname,
							PG_NAMEDATALEN);
				}
			}
		}
	}

	/* Parse include-only-extension array */
	JSON_Array *includeExtArray =
		json_object_get_array(jsFilterObj, "include-only-extension");

	if (includeExtArray != NULL)
	{
		size_t count = json_array_get_count(includeExtArray);
		filters->includeOnlyExtensionList.count = count;

		if (count > 0)
		{
			filters->includeOnlyExtensionList.array =
				(SourceFilterExtension *) calloc(count, sizeof(SourceFilterExtension));

			if (filters->includeOnlyExtensionList.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(jsFilter);
				return false;
			}

			for (size_t i = 0; i < count; i++)
			{
				const char *extname = json_array_get_string(includeExtArray, i);

				if (extname != NULL)
				{
					strlcpy(filters->includeOnlyExtensionList.array[i].extname,
							extname,
							PG_NAMEDATALEN);
				}
			}
		}
	}

	/* Parse exclude-extension array */
	JSON_Array *excludeExtArray =
		json_object_get_array(jsFilterObj, "exclude-extension");

	if (excludeExtArray != NULL)
	{
		size_t count = json_array_get_count(excludeExtArray);
		filters->excludeExtensionList.count = count;

		if (count > 0)
		{
			filters->excludeExtensionList.array =
				(SourceFilterExtension *) calloc(count, sizeof(SourceFilterExtension));

			if (filters->excludeExtensionList.array == NULL)
			{
				log_error(ALLOCATION_FAILED_ERROR);
				json_value_free(jsFilter);
				return false;
			}

			for (size_t i = 0; i < count; i++)
			{
				const char *extname = json_array_get_string(excludeExtArray, i);

				if (extname != NULL)
				{
					strlcpy(filters->excludeExtensionList.array[i].extname,
							extname,
							PG_NAMEDATALEN);
				}
			}
		}
	}

	/* Parse table lists (exclude-table, exclude-table-data, etc.) */
	struct section
	{
		const char *name;
		SourceFilterTableList *list;
	};

	struct section sections[] = {
		{ "exclude-table", &(filters->excludeTableList) },
		{ "exclude-table-data", &(filters->excludeTableDataList) },
		{ "exclude-index", &(filters->excludeIndexList) },
		{ "include-only-table", &(filters->includeOnlyTableList) },
		{ NULL, NULL },
	};

	for (int i = 0; sections[i].name != NULL; i++)
	{
		const char *sectionName = sections[i].name;
		SourceFilterTableList *list = sections[i].list;

		JSON_Array *tableArray = json_object_get_array(jsFilterObj, sectionName);

		if (tableArray != NULL)
		{
			size_t count = json_array_get_count(tableArray);
			list->count = count;

			if (count > 0)
			{
				list->array =
					(SourceFilterTable *) calloc(count, sizeof(SourceFilterTable));

				if (list->array == NULL)
				{
					log_error(ALLOCATION_FAILED_ERROR);
					json_value_free(jsFilter);
					return false;
				}

				for (size_t j = 0; j < count; j++)
				{
					JSON_Object *tableObj = json_array_get_object(tableArray, j);

					if (tableObj != NULL)
					{
						const char *schema = json_object_get_string(tableObj, "schema");
						const char *name = json_object_get_string(tableObj, "name");

						if (schema != NULL)
						{
							strlcpy(list->array[j].nspname, schema, PG_NAMEDATALEN);
						}

						if (name != NULL)
						{
							strlcpy(list->array[j].relname, name, PG_NAMEDATALEN);
						}
					}
				}
			}
		}
	}

	json_value_free(jsFilter);

	return true;
}
