### Create table - User
create table keepnote.User(
user_id varchar(10) not null,
user_name varchar(20),
user_added_date date,
user_password varchar(20),
user_mobile int(10),
primary key (user_id));

### Create table - Note
create table keepnote.Note(
note_id int(10) not null,
note_title varchar(20),
note_content varchar(30),
note_status varchar(20),
note_creation_date date,
primary key (note_id));

### Create table - Category
create table keepnote.Category(
category_id int(10) not null,
category_name varchar(20),
category_descr varchar(30),
category_creation_date date,
category_creator varchar(20),
primary key (category_id)
);

### Create table - Reminder
create table keepnote.Reminder(
reminder_id int(10) not null,
reminder_name varchar(20),
reminder_descr varchar(30),
reminder_type varchar(20),
reminder_creation_date date,
reminder_creator varchar(20),
primary key (reminder_id)
);

### Create table - NoteReminder
create table keepnote.NoteReminder(
notereminder_id int(10) not null,
note_id int(10),
reminder_id int(10),
primary key (notereminder_id),
foreign key (note_id) references Note (note_id),
foreign key (reminder_id) references Reminder(reminder_id)
);

### Create table - NoteCategory
create table keepnote.NoteCategory(
notecategory_id int(10) not null,
note_id int(10),
category_id int(10),
primary key (notecategory_id),
foreign key (note_id) references Note (note_id),
foreign key (category_id) references Category(category_id)
);

### Create table - UserNote
create table keepnote.UserNote(
usernote_id int(10) not null,
user_id varchar(10),
note_id int(10),
primary key (usernote_id),
foreign key (user_id) references User(user_id),
foreign key (note_id) references Note(note_id)
);

### inserting data into table - User
insert into keepnote.User
(user_id,user_name,user_added_date,user_password,user_mobile)
values
('U001','user1',curdate(),'p4ssw0rd',1111111111);
insert into keepnote.User
(user_id,user_name,user_added_date,user_password,user_mobile)
values
('U002','user2',curdate(),'p4ssw0rd',2222222222);


### inserting data into table - Note
insert into keepnote.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(1,'note_1','some note details of note_1','Open',curdate());
insert into keepnote.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(2,'note_2','another note details of note_2','Open',curdate());


### inserting data into table - Category
insert into keepnote.Category
(category_id,category_name,category_descr,category_creation_date,category_creator)
values
(1,'General','All general category notes',curdate(),'user_1');
insert into keepnote.Category
(category_id,category_name,category_descr,category_creation_date,category_creator)
values
(2,'Personal','Notes that are personal',curdate(),'user_1');

### inserting data into table - Reminder
insert into keepnote.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(1,'Meeting','Meeting Reminder of con-call','Urgent',curdate(),'user_2');
insert into keepnote.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(2,'ClientMeet','In-person meeting with clients','Critical',curdate(),'user_2');


### inserting data into table - NoteReminder
insert into keepnote.NoteReminder
(notereminder_id,note_id,reminder_id)
values
(1,1,1);


### inserting data into table - NoteCategory
insert into keepnote.NoteCategory
(notecategory_id,note_id,category_id)
values
(1,1,1);

### inserting data into table - UserNote
insert into keepnote.UserNote
(usernote_id,user_id,note_id)
values
(1,'U001',1);


### Fetch the row from User table based on Id and Password.
select * from keepnote.User where user_id='U001' and user_password='p4ssw0rd';

### Fetch all the rows from Note table based on the field note_creation_date.
select * from keepnote.Note where note_creation_date='2019-05-14';

### Fetch all the Categories created after the particular Date.
select * from keepnote.Category where category_creation_date > '2018-12-31';

### Fetch all the Note ID from UserNote table for a given User.
select note_id from keepnote.UserNote where user_id='U001';

### Write Update query to modify particular Note for the given note Id.
update keepnote.Note set note_title = 'updated_title', note_content = 'note content got updated'
where note_id=2;

### Fetch all the Notes from the Note table by a particular User.
select n.note_id,n.note_title,n.note_content,n.note_status,n.note_creation_date
from keepnote.Note n, keepnote.User u, keepnote.UserNote un where n.note_id = un.note_id
and u.user_id = un.user_id and u.user_id='U001';

### Fetch all the Notes from the Note table for a particular Category.
select n.note_id,n.note_title,n.note_content,n.note_status,n.note_creation_date
from keepnote.Note n, keepnote.Category c, keepnote.NoteCategory nc where n.note_id = nc.note_id
and c.category_id = nc.category_id and c.category_id=1;

### Fetch all the reminder details for a given note id.
select r.reminder_id,r.reminder_name,r.reminder_descr,r.reminder_type,
r.reminder_creation_date,r.reminder_creator
from keepnote.Reminder r, keepnote.Note n, keepnote.NoteReminder nr
where r.reminder_id = nr.reminder_id and n.note_id = nr.note_id
and n.note_id = 1;

### Fetch the reminder details for a given reminder id.
select r.reminder_id,r.reminder_name,r.reminder_descr,r.reminder_type,
r.reminder_creation_date,r.reminder_creator
from keepnote.Reminder r where r.reminder_id = 1;

### Write a query to create a new Note from particular User (Use Note and UserNote tables - insert statement).
insert into keepnote.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(3,'new_note','this is a new note created','Open',curdate());
insert into keepnote.UserNote
(usernote_id,user_id,note_id)
values
(2,'U002',3);

### Write a query to create a new Note from particular User to particular Category(Use Note and NoteCategory tables - insert statement)
insert into keepnote.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(4,'another_new_note','this is another new note created with a category','Open',curdate());
insert into keepnote.UserNote
(usernote_id,user_id,note_id)
values
(3,'U001',4);
insert into keepnote.NoteCategory
(notecategory_id,category_id,note_id)
values
(2,2,4);

### Write a query to set a reminder for a particular note (Use Reminder and NoteReminder tables - insert statement)
insert into keepnote.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(3,'new_reminder','a new reminder created with type','Meeting',curdate(),'user_3');
insert into keepnote.NoteReminder
(notereminder_id,note_id,reminder_id)
values
(2,2,3);

### Write a query to delete particular Note added by a User(Note and UserNote tables - delete statement)
delete from keepnote.UserNote where user_id='U002' and note_id=3;
delete from keepnote.Note where note_id=3;

### Write a query to delete particular Note from particular Category(Note and NoteCategory tables - delete statement)
delete from keepnote.NoteCategory where note_id=4 and category_id=2;
delete from keepnote.UserNote where note_id=4;
delete from keepnote.Note where note_id=4;

DELIMITER $$
create trigger keepnote.before_note_delete
	before delete on keepnote.Note
    for each row
    begin
	delete from keepnote.UserNote where note_id= OLD.note_id;
    delete from keepnote.NoteCategory where note_id= OLD.note_id;
    delete from keepnote.NoteReminder where note_id= OLD.note_id;
    END$$
DELIMITER ;

create trigger keepnote.before_user_delete
	before delete on keepnote.User
    for each row
	delete from keepnote.UserNote where user_id= OLD.user_id;
