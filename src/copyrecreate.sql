create or replace procedure table_recreate_copy(SRC_COLS STRING, TGT_COLS STRING, TGT_TABLE STRING, SRC_TABLE STRING)
	returns varchar
	language javascript
	as
	$$
	var old_arr = SRC_COLS.split(',');
	var k = 0, l=0;
	var old_col_names=[];
	var old_col_types=[];
	var new_col_names=[];
	var new_col_types=[];
	while(k<old_arr.length) {
	old_col_names[k] = old_arr[k].split(' ')[0];
	old_col_types[k] = old_arr[k].split(' ')[1];
	k++;
	}

	var new_arr = TGT_COLS.split(',');
	while(l<new_arr.length) {
	new_col_names[l] = new_arr[l].split(' ')[0];
	new_col_types[l] = new_arr[l].split(' ')[1];
    l++;
	}

	var insert_cmd;
	var insert_cmd_first = `insert into ` + TGT_TABLE + `(`;
    var insert_cmd_sec = `select `;
    var i =0;
	while(i<l && i<k) {

		insert_cmd_first = insert_cmd_first + new_col_names[i] + `, `;

			if(old_col_types[i] == new_col_types[i]) {

				insert_cmd_sec = insert_cmd_sec +  old_col_names[i] + `, `;
			}
			else {
			insert_cmd_sec = insert_cmd_sec + `cast (` + old_col_names[i] + `as ` + new_col_types[i] +`), `;
			}
            i++;
	}
	if(l>k){
		while(i<l) {
			insert_cmd_first = insert_cmd_first + new_col_names[i] + `, `;
			insert_cmd_sec = insert_cmd_sec + `default_value, `;
			i++;
		 }
	}
	var get_unchanged_sql = `select a.column_name
		from INFORMATION_SCHEMA.columns a
		JOIN INFORMATION_SCHEMA.columns b
		ON a.column_name = b.column_name
		AND a.data_type = b.data_type
		where a.table_schema = 'MTD_TEST'
		AND b.table_schema = 'MTD_TEST'
		AND a.table_name = 'TGT_SAT_MAP'
		AND b.table_name = 'MTD_SAT_MAP';`;
	var res = snowflake.execute({sqlText:get_unchanged_sql}); 
	var unchanged_cols='';
	while(res.next()) {
		unchanged_cols = unchanged_cols + res.getColumnValue(1) + `, `;
	}

	insert_cmd_first = insert_cmd_first + unchanged_cols;
	insert_cmd_first = insert_cmd_first.substring(0, insert_cmd_first.length-2) +`) `;
	insert_cmd_sec = insert_cmd_sec + unchanged_cols;
	insert_cmd_sec = insert_cmd_sec.substring(0, insert_cmd_sec.length-2) + ` from ` + SRC_TABLE +`;`;
	var sqlinsert = insert_cmd_first + insert_cmd_sec;
	snowflake.execute({sqlText:sqlinsert});
	return sqlinsert;
	$$
	;


/*
Details about the above Procedure

1st parameter - A comma separated String of SRC Columns with DataTypes
2nd Parameter - A comma separated String of Target Columns with DataTypes
3rd Parameter - Fully Qualified Name of Target Table
4th Parameter - Fully Qualified Name of Source Table

Examples Call to this procedure

//call table_recreate_copy('SRC_TABLE_ID NUMBER,SAT_COLUMN_ID STRING', 'SRCD_TABLE_ID NUMBER,SAT_COL STRING,VER_ID NUMBER', 'TGT_SAT_MAP', 'MTD_SAT_MAP');

//call table_recreate_copy('SRC_TABLE_ID NUMBER,SAT_COLUMN_ID STRING,VERSION_ID NUMBER', 'SRCD_TABLE_ID NUMBER,SAT_COL STRING,VER_ID NUMBER', 'TGT_SAT_MAP', 'MTD_SAT_MAP');