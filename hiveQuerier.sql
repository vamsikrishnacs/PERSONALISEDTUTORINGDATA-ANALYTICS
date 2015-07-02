select distinct id ,username from auth_user where (id is not NULL and is_staff=1 and is_active=1) order by username;

-- how many users are female?
select gender,count(distinct user_id) from auth_userprofile where gender!="NULL" and user_id is not null and gender!="" group by gender;

-- how many students pursuing their bachelors have registered
select id,name from auth_userprofile where (id is not NULL and level_of_education='b') order by id,name limit 10;

-- how many have registered for a particular course are born after 1980
select id,name,gender from auth_userprofile where year_of_birth > 1980 limit 3;

-- How many courses a particular user is registered for?
select user_id,count(course_id) from student_courseenrollment where (id is not null) GROUP BY user_id;

--  How many students have a mode as honored?
select mode,count(mode) from student_courseenrollment where (mode='honor') group by mode;

--  How many students have enrolled in a particular course?
select course_id,count(user_id) from student_courseenrollment where (id is not null) group by course_id;

-- what was the last time when a student has registered for a course?
select distinct user_id,course_id,created from student_courseenrollment where id is not NULL order by user_id DESC,created DESC;

-- in which subject a student has registered more than once?
select user_id from student_courseenrollment group by user_id having count(course_id) > 1;

-- progress when registered for a course more than once?(courseware_studentmodule))

-- how many students have their enrolled un-verified?
select count(distinct user_id) from student_courseenrollment where (mode not like 'verify' and id is not null);

-- how many who got less marks have registered again for the same course?


-- how many student who have good marks have registered for the advanced level of a course?

-- to find the actual id of a user?
select id,username from user_id_map where id is not null;

-- percentage of course a student has failed in?(doesnt get a certificate)
-- certificates_generatedcertificate will be done after generating some data for it

-- courseware_studentmodule
-- Q1). What is the highest grade obtained by any student in a particular module ?
select student_id,max(grade) from courseware_studentmodule where id is not null group by student_id;

-- 2). How many chapters have been solved by the students in a particular course ?
select student_id,course_id,count(module_type) from courseware_studentmodule where id is not null and module_type='chapter' group by student_id,course_id;

-- Total amount of time student spends ona particular course
-- distinct not working??
select distinct(student_id),modified from courseware_studentmodule where id is not null order by modified desc;

-- grade obtained by student who has viewed the minimum number of videos in a particular course






