#ifndef TARGET_STATE_H
#define TARGET_STATE_H

#include "copydb.h"
#include "pgsql.h"

void copydb_target_inspect(CopyDataSpec *specs);
void copydb_target_report(CopyDataSpec *specs, PGSQL *failed);

#endif /* TARGET_STATE_H */
