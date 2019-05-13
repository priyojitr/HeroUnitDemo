create table assignment0.User(
user_id varchar(10) not null,
user_name varchar(20),
user_added_date date,
user_password varchar(20),
user_mobile int(10),
primary key (user_id));

create table assignment0.Note(
note_id int(10) not null,
note_title varchar(20),
note_content varchar(30),
note_status varchar(20),
note_creation_date date,
primary key (note_id));

create table assignment0.Category(
category_id int(10) not null,
category_name varchar(20),
category_descr varchar(30),
category_creation_date date,
category_creator varchar(20),
primary key (category_id)
);

create table assignment0.Reminder(
reminder_id int(10) not null,
reminder_name varchar(20),
reminder_descr varchar(30),
reminder_type varchar(20),
reminder_creation_date date,
reminder_creator varchar(20),
primary key (reminder_id)
);

create table assignment0.NoteReminder(
notereminder_id int(10) not null,
note_id int(10),
reminder_id int(10),
primary key (notereminder_id),
foreign key (note_id) references Note (note_id),
foreign key (reminder_id) references Reminder(reminder_id)
);

create table assignment0.NoteCategory(
notecategory_id int(10) not null,
note_id int(10),
category_id int(10),
primary key (notecategory_id),
foreign key (note_id) references Note (note_id),
foreign key (category_id) references Category(category_id)
);

create table assignment0.UserNote(
usernote_id int(10) not null,
user_id varchar(10),
note_id int(10),
primary key (usernote_id),
foreign key (user_id) references User(user_id),
foreign key (note_id) references Note(note_id)
);

insert into assignment0.User
(user_id,user_name,user_added_date,user_password,user_mobile)
values
('U001','Sam',curdate(),'P@ssw0rd',886754123);
insert into assignment0.User
(user_id,user_name,user_added_date,user_password,user_mobile)
values
('U002','Ram',curdate(),'P@ssw0rd',987867564);

insert into assignment0.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(1,'Account Name','First Data','Open',curdate());
insert into assignment0.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(2,'Project Name','Global Gateway','Open',curdate());


insert into assignment0.Category
(category_id,category_name,category_descr,category_creation_date,category_creator)
values
(1,'General','General Notes',curdate(),'Debanjan');
insert into assignment0.Category
(category_id,category_name,category_descr,category_creation_date,category_creator)
values
(2,'Urgent','Urgent Notes',curdate(),'Debanjan');

insert into assignment0.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(1,'Meeting','Meeting Reminder','Urgent',curdate(),'Debanjan');
insert into assignment0.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(2,'Party','Party Reminder','Fun',curdate(),'Debanjan');

insert into assignment0.NoteReminder
(notereminder_id,note_id,reminder_id)
values
(1,1,1);



insert into assignment0.NoteCategory
(notecategory_id,note_id,category_id)
values
(1,1,1);


insert into assignment0.UserNote
(usernote_id,user_id,note_id)
values
(1,'U001',1);


###Fetch the row from user table based on the Id and Password####
select * from assignment0.User where user_id='U001' and user_password='P@ssw0rd';

##Fetch all the rows from the Note table based on teh field note_creation_date####
select * from assignment0.Note where note_creation_date='2018-12-14';

###Fetch all the categories created after these particular date###
select * from assignment0.Category where category_creation_date > '2018-12-13';

###Fetch all the note id from UserNote table for a given user###
select note_id from assignment0.UserNote where user_id='U001';

###Update query to modify note for a given note id ####
update assignment0.Note set note_title = 'Party', note_content = 'Birthday Cake'
where note_id=2;

###Fetch all the notes from Note table for a particular user###
select n.note_id,n.note_title,n.note_content,n.note_status,n.note_creation_date
from assignment0.Note n, assignment0.User u,assignment0.UserNote un where n.note_id = un.note_id
and u.user_id = un.user_id and u.user_id='U001';

###Fetch all the notes from Note table for a particular category###
select n.note_id,n.note_title,n.note_content,n.note_status,n.note_creation_date
from assignment0.Note n, assignment0.Category c,assignment0.NoteCategory nc where n.note_id = nc.note_id
and c.category_id = nc.category_id and c.category_id=1;

###Fetch the reminder details for a given note id###
select r.reminder_id,r.reminder_name,r.reminder_descr,r.reminder_type,
r.reminder_creation_date,r.reminder_creator
from assignment0.Reminder r, assignment0.Note n, assignment0.NoteReminder nr
where r.reminder_id = nr.reminder_id and n.note_id = nr.note_id
and n.note_id = 1;

###Fetch the reminder details for a given reminder id###
select r.reminder_id,r.reminder_name,r.reminder_descr,r.reminder_type,
r.reminder_creation_date,r.reminder_creator
from assignment0.Reminder r where r.reminder_id = 1;

###Write a query to create note from particular user####
insert into assignment0.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(3,'ProdRelease','Production Release','Open',curdate());
insert into assignment0.UserNote
(usernote_id,user_id,note_id)
values
(2,'U002',3);

###Write a query to create note from particular user to particular category####
insert into assignment0.Note
(note_id,note_title,note_content,note_status,note_creation_date)
values
(4,'Party','Project Party','Open',curdate());
insert into assignment0.UserNote
(usernote_id,user_id,note_id)
values
(3,'U001',4);
insert into assignment0.NoteCategory
(notecategory_id,category_id,note_id)
values
(2,2,4);

##Write a query to set a reminder for particular Note###
insert into assignment0.Reminder
(reminder_id,reminder_name,reminder_descr,reminder_type,reminder_creation_date,reminder_creator)
values
(3,'ProjParty','Project Party Reminder','Party',curdate(),'Manish');
insert into assignment0.NoteReminder
(notereminder_id,note_id,reminder_id)
values
(2,2,3);


##Write a query to delete particular Note added by a User##
delete from assignment0.UserNote where user_id='U002' and note_id=3;
delete from assignment0.Note where note_id=3;

##Write a query to delete particular Note for particular category##
delete from assignment0.NoteCategory where note_id=4 and category_id=2;
delete from assignment0.UserNote where note_id=4;
delete from assignment0.Note where note_id=4;


DELIMITER $$
create trigger assignment0.before_note_delete
	before delete on assignment0.Note
    for each row
    begin
	delete from assignment0.UserNote where note_id= OLD.note_id;
    delete from assignment0.NoteCategory where note_id= OLD.note_id;
    delete from assignment0.NoteReminder where note_id= OLD.note_id;
    END$$
DELIMITER ;


create trigger assignment0.before_user_delete
	before delete on assignment0.User
    for each row
	delete from assignment0.UserNote where user_id= OLD.user_id;
