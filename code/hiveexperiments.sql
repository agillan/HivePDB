set yarn.nodemanager.vmem-check-enabled=false;
set yarn.nodemanager.pmem-check-enabled=false;
set hive.exec.parallel=false;


set yarn.nodemanager.resource.cpu-vcores=4;
set mapred.tasktracker.map.tasks.maximum=4;


add file /home/local/zslf023/Transform.py;
add JARS file:///home/local/zslf023/hivexmlserde-1.0.5.1.jar;
set hive.hadoop.supports.splittable.combineinputformat=true;
set hive.merge.mapredfiles=true;
add JARS file:///home/local/zslf023/brickhouse-0.7.0-SNAPSHOT.jar;
set hive.cli.print.current.db=true;

set io.file.buffer.size=65536;
set dfs.namenode.handler.count=64;
set mapred.job.tracker.handler.count=64;
set dfs.datanode.handler.count=8;
SET mapred.child.java.opts=-Xmx4096m;
set mapred.child.ulimit=-8388608;



CREATE TEMPORARY FUNCTION collect AS 'brickhouse.udf.collect.CollectUDAF';

DROP DATABASE IF EXISTS pdb5k CASCADE;
CREATE DATABASE IF NOT EXISTS pdb5k;
USE pdb5k;

CREATE EXTERNAL TABLE pdb_xml_load(pdbid STRING, record ARRAY<string>)
ROW FORMAT SERDE 'com.ibm.spss.hive.serde2.xml.XmlSerDe'
WITH SERDEPROPERTIES (
"column.xpath.pdbid"="/*[local-name()='datablock']/@datablockName",
"column.xpath.record"="/*[local-name()='datablock']/*[local-name()='category_atom_record']/*[local-name()='atom_record']/text()"
)
STORED AS
INPUTFORMAT 'com.ibm.spss.hive.serde2.xml.XmlInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.IgnoreKeyTextOutputFormat'
LOCATION "/user/zslf023/pdb/fivek"
TBLPROPERTIES (
"xmlinput.start"="<PDBx:datablock ",
"xmlinput.end"="</PDBx:datablock>"
);


CREATE VIEW splitPDB AS
SELECT split(pdbid,"-")[0] as pdbid, split(entry,"\\p{Zs}+") as record
FROM pdb_xml_load LATERAL VIEW explode(record) splitTable AS entry
CLUSTER BY pdbid;

CREATE VIEW error_records
AS
SELECT DISTINCT pdbid from splitpdb WHERE
record[0] != "HETATM" 
AND
(size(record) !=20
OR record[2] = "."
OR record[2] = "?"
OR record[4] = "."
OR record[8] = "."
OR record[8] = "?"
OR record[11] = "."
OR record[11] = "?"
OR record[13] = "."
OR record[13] = "?"
OR record[14] = "."
OR record[14] = "?"
OR record[15] = "."
OR record[15] = "?");


CREATE TABLE atom_coordinates AS
SELECT splitpdb.pdbid, cast(record[1] as int) as pdbx_PDB_model_num, record[0] as group_pdb, record[11] as label_atom_id, record[8] as label_comp_id, record[2] as label_asym_id, cast(record[4] as int) as label_seq_id, cast(record[13] as double) as CartnX,cast(record[14] as double) as CartnY,cast(record[15] as double) as CartnZ
FROM splitpdb
LEFT OUTER JOIN error_records
ON (splitpdb.pdbid = error_records.pdbid)
WHERE error_records.pdbid IS null
and record[0] = "ATOM"
DISTRIBUTE BY splitpdb.pdbid;


CREATE TABLE coordinates_grouped AS
SELECT pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id, label_comp_id, collect(label_atom_id,ARRAY(cartnx,cartny,cartnz)) as atoms
FROM atom_coordinates
WHERE label_atom_id in ("N","CA","C")
AND label_comp_id in ('ARG', 'HIS', 'LYS', 'ASP', 'GLU', 'SER', 'THR','ASN', 'GLN', 'CYS', 'GLY', 'PRO', 'ALA', 'ILE', 'LEU', 'MET', 'PHE', 'TRP', 'TYR', 'VAL')
GROUP BY pdbid, pdbx_PDB_model_num, label_asym_id,label_seq_id,label_comp_id;

create view noatoms as select distinct pdbid from coordinates_grouped where size(atoms) != 3;

create table residue_coordinates as
SELECT  t1.pdbid, t1.pdbx_PDB_model_num, t1.label_asym_id, t1.label_seq_id, t1.label_comp_id, t1.atoms
FROM coordinates_grouped t1
LEFT OUTER JOIN noatoms 
ON (t1.pdbid = noatoms.pdbid) 
WHERE noatoms.pdbid IS NULL;

GRANT SELECT ON TABLE atom_coordinates TO ROLE public;
GRANT SELECT ON TABLE residue_coordinates TO ROLE public;

--FAILED: RuntimeException java.io.IOException: Failed on local exception: java.nio.channels.ClosedByInterruptException; Host Details : local host is: "bigdata.hadoop.local/10.0.0.11"; destination host is: "bigdata":8020; 
