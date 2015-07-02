use amazonserver;
select username,problem_id,sum(grade),sum(max_grade) from(select username,problem_id,grade,max_grade from pi_log_table where course_id='IITBombayX/CS101.1x/2T2014' and problem_id!='null' and max_grade!='null'order by username,problem_id)t1 group by username,problem_id;
