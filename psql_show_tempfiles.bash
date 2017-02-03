#!/bin/bash
#
# BASH script (bash version 4 minimum required) 
# Displays queries that generate temporary files in PostgreSQL
# Tested with pg92 => pg96
# Version 0.1 / Nicolas GOLLET ng@ng.pe

set -u

coprocess_pid=
refreshoff=0

# Start psql has coproc
function coprocpsql ()
{
	coproc psql -U postgres -p 5432 --quiet --no-align --no-readline --tuples-only  -P footer=off --field-separator ";" postgres
	coprocess_pid=${COPROC_PID}
  exec 3<&${COPROC[0]}
	# send all line to "coprocpsqlprint" function
  while IFS= read -ru 3 x; do coprocpsqlprint "$x"; done &
}

function coprocpsqlprint ()
{

	# print line from PSQL command line
	if [ "${refreshoff}" -eq "0" ]; then
		rpid=$(cut -d ";" -f1 <<< $1)
		rdatabase=$(cut -d ";" -f2 <<< $1)
		rduration=$(cut -d ";" -f4 <<< $1)
		ruser=$(cut -d ";" -f5 <<< $1)
		rfilesize=$(cut -d ";" -f7 <<< $1)
		rfilenum=$(cut -d ";" -f8 <<< $1)
		rquery=$(cut -d ";" -f9- <<< $1)

		echo -e "PID : \e[1m${rpid}\e[0m \t Database/User : \e[1m${rdatabase}\e[0m/\e[1m${ruser}\e[0m \n\
Tempfile size : \e[1m${rfilesize}\e[0m (${rfilenum} file(s)) \t Running : \e[1m${rduration}\e[0m sec\n\
Query : \n********************************** \n\e[33m${rquery}\e[39m\n********************************** \n"

	fi

}

function coprocpsqlquery ()
{
	echo -e "\e[31m------\e[91m" $(date) "\e[31m------\e[39m\n"
  # Send query to psql coproc
  echo "${sql_query}" >&${COPROC[1]}
}

# SQL Query for Temp files comes from https://github.com/gleu/pgstats/ project
sql_query=$(cat <<EOF
SELECT pg_stat_activity.pid AS pid,
       CASE
           WHEN LENGTH(pg_stat_activity.datname) > 16 THEN SUBSTRING(pg_stat_activity.datname
           FROM 0
           FOR 6)||'...'||SUBSTRING(pg_stat_activity.datname
           FROM '........$')
           ELSE pg_stat_activity.datname
       END AS DATABASE,
        pg_stat_activity.client_addr AS client,
        EXTRACT(epoch
               FROM (NOW() - pg_stat_activity.query_start)) AS duration,
        pg_stat_activity.usename AS USER,
        pg_stat_activity.state AS state,
        pg_size_pretty(pg_temp_files.sum) AS temp_file_size,
        pg_temp_files.count AS temp_file_num,
        pg_stat_activity.query AS query
FROM pg_stat_activity AS pg_stat_activity
INNER JOIN
  (SELECT unnest(regexp_matches(agg.tmpfile, 'pgsql_tmp([0-9]*)')) AS pid,
          SUM((pg_stat_file(agg.dir||'/'||agg.tmpfile)).size),
          count(*)
   FROM
     (SELECT ls.oid,
             ls.spcname,
             ls.dir||'/'||ls.sub AS dir,
             CASE gs.i
                 WHEN 1 THEN ''
                 ELSE pg_ls_dir(dir||'/'||ls.sub)
             END AS tmpfile
      FROM
        (SELECT sr.oid,
                sr.spcname,
                'pg_tblspc/'||sr.oid||'/'||sr.spc_root AS dir,
                pg_ls_dir('pg_tblspc/'||sr.oid||'/'||sr.spc_root) AS sub
         FROM
           (SELECT spc.oid,
                   spc.spcname,
                   pg_ls_dir('pg_tblspc/'||spc.oid) AS spc_root,
                   trim(TRAILING E'\n '
                        FROM pg_read_file('PG_VERSION')) AS v
            FROM
              (SELECT oid,
                      spcname
               FROM pg_tablespace
               WHERE spcname !~ '^pg_') AS spc) sr
         WHERE sr.spc_root ~ ('^PG_'||sr.v)
           UNION ALL
           SELECT 0,
                  'pg_default',
                  'base' AS dir,
                  'pgsql_tmp' AS sub
           FROM pg_ls_dir('base') AS l WHERE l='pgsql_tmp' ) AS ls,

        (SELECT generate_series(1,2) AS i) AS gs
      WHERE ls.sub = 'pgsql_tmp') agg
   GROUP BY 1) AS pg_temp_files ON (pg_stat_activity.pid = pg_temp_files.pid::int)
WHERE pg_stat_activity.pid <> pg_backend_pid()
ORDER BY EXTRACT(epoch
                 FROM (NOW() - pg_stat_activity.query_start)) DESC;
EOF
)

echo "psql_show_tempfiles v 0.1"
# start PSQL command has coproc
coprocpsql
echo psql coproc pid : ${coprocess_pid}
echo auto refresh is : 2sec / Press p to pause, q to exit
sleep 1

while true; do
  if kill -0 ${coprocess_pid} 2>/dev/null; then         # has psql exited?
 	     # send sql query
       coprocpsqlquery
	     read -s -t 2 -n 1 key # sleep and key grabbing
       if [[ "$key" = "p" ]]; then
             refreshoff=1
             keys=
	           read -n 1 -s -p "Refresh is paused... Press any key to continue"
	           echo -e "\n"
       elif [[ "$key" = "q" ]]; then
	           refreshoff=1
	           echo "byebye..."
             exit 0
	     fi
	   
  else
    	 wait ${coprocess_pid}
    	 status=$?
    	 echo "PSQL subprocess terminate ($status)"
    	 exit $status
  fi
done
