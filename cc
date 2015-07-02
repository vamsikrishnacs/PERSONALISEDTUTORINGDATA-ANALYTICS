use amazonserver;
select username,count(distinct(session)) from log_table where course_id='IITBombayX/CS101.1x/2T2014' group by username;
