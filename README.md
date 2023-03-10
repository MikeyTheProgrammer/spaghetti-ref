**How to run spaghetti on a given Excel file(s)?
Very simple:**

1. Make sure that each Excel file contains the column 'מספר אפידימיולוגי'. 
    
    **Invalid files, e.g., files without the column 'מספר אפידימיולוגי', will be skipped.**

2. Put all files in the directory *\\\\fsmtt\public\Idf-Data\פיצוח\Pakar\spaghetti\in*.
3. Run the script spaghetti.py.
    
    *The current defaults are:*
    - levels to explore up: 2
    - levels to explore down: 10
    - days to explore back: 21
    - send email: enabled
    - sender: 'gilad.goldreich@moh.gov.il'
    - receiver: '5079371@moh.gov.il' (Ezer Biron)
4. The script will save the results for each (valid) file in the directory *\\\\fsmtt\public\Idf-Data\פיצוח\Pakar\spaghetti\out*,
and will send an email with the results if enabled.

    *Each row in the output file represents a contact, and contains the following attributes:*
    - ids (epidemiological number if exists, otherwise identity number)
    - names
    - ages
    - work places (from xrm)
    - was ever positive indicators
    - first positive result test dates
    - contact date


    תרשים זרימה:

ראשית המערכת עולה בתצוגת אתר סטרימליט.
לאחר מכן על המשתמש להכניס פרמטרים, אשר ערכם נשמר בתוך קובץ תצוגת האתר, והם ישמשו אותנו לכיוון המערכת.
לאחר שמתקבל קלט משתמש, המערכת מזהה את אותו קלט, ומופעל קובץ הspagehtti.py אשר מתחיל לעבוד.
הוא משתמש ב"sql connector" ומבצע שליפת SQL על הנתונים אשר הוכנסו על ידי המשתמש באמצעות קובץ אקסל (מספרים מזהים).
 לאחר שהשליפה מסתיימת, הקובץ spaghetti.py מסדר אותם באקסל ומקשר בין מאומת למגע, ומייצא אותו באמצעות פונקציה שמאפשרת להוריד את קובץ האקסל ממערך המידע (data frame) שנוצר לנו.

![Alt text](Untitled%20Diagram.drawio.png)


מה זה Spaghetti?
ספגטי היא מערכת אשר מקשרת מאומת למגעים שלו, בהתבסס על שכנים, קרובי משפחה הגרים איתו, ומגעים נוספים אשר מדווחים בחקירה האפידמיולוגית המבוצעת על אותו מאומת.

המערכת ראשית מקבלת קובץ אקסל עם מספרי האפים/פלאפון/זהות אשר המשתמש רוצה להריץ עליהם את המערכת, ומחזירה את המגעים המקושרים לאותו מאומת.

המערכת מתבססת על שליפות SQL מאחסון המידע "BI_Corona", ומוציאה את המידע הנחוץ על כל מגע של אותו מאומת על מנת לבצע חקירה מעמיקה.

כיום, קובץ הספגטי המיוצא הוא בעל שתי שימושים, האחד, כקובץ אקסל המכיל מידע על מגעים ומאומתים.

השני, הוא מיועד למערכת Nexum, אשר לוקחת את אותו קובץ אקסל, ומציגה אותו בצורה ויזואלית.

מהם רכיבי הפרויקט?
-שרת
- ממשק משתמש מבוסס WEB
API -
- שאילתות ממאגרי הנתונים של משרד הבריאות
- מאגרי מידע.
- סטרימליט

אילו טכנולוגיות נבחרו ליישם את הפרויקט?
-	SQL
-	Python

שימוש בספריות:
json
hashlib
os
mimetypes
smtplib
pandas
numpy
datetime
pyodbc
networkx
streamlit
base64
BytesI0
מבנה הפרויקט:

-	website_functions.py
הסבר: קובץ זה מכיל פונקציות קריטיות להפעלת המערכת.

get_running_status()- הפונקציה הזאת בודקת אם יש ספגטי פעיל במערכת כרגע
change_running_status() -  הפונקציה הזאת מעדכנת את קובץ הלוגים לסטטוס הנוכחי

increase_run_times()  -  הפונקציה הזאת מעדכנת את הלוג בנוגע למספר הריצות שהתרחשו.

download_link_from_df  - פונקציה זו יוצרת לינק הורדה לקובץ האקסל אשר יוצא על ידי ספגטי

download_zip_file - פונקציה זו מייצאה קובץ זיפ עם הקונפיגורציה(הגדרות התאמה) של מלטגו

gif - פונקציה זו מאפשרת למפתח ליצור גיף ולשלב אותו בקוד בצורה פשוטה.

run_spaghetti - פונקציה זו מבשלת ספגטי (מריצה את המערכת), פונקציה זו מקבלת את כל הפרמטרים הנדרשים שהוכנסו על ידי המשתמש.

-	spaghetti_website.py
הסבר: קובץ זה מכיל את החלק שבו אפשר להפעיל את המערכת (אתר)

page_start -הפונקציה הזו מוסיפה כותרת ותוכן לאתר

dates_options-הפונקציה האחראית על הגדרת טווח התאריכים

levels_options-הפונקציה הזו ממגדירה כמה רמות למעלה ולמטה אפשר לרדת/לעלות

output_settings-הפונקציה הזו מגדירה את דרך קבלת הפלט,קובץ אקסל למחשב או קובץ אקסל למייל

check_df_input-הפונקציה הזו בודקת אם הקלט תואם לשבלונה

send_and_save_to_mail- פומקציה זו שולחת את קובץ האקסל למייל

handle_output- הפונקציה הזו מנהלת את הפלט של הספגטי

input_options-הפונקציה הזו היא הפונקציה המרכזית של ספגטי, היא נותנת לך להעלות את קובץ השבלונה ובדקת אם יש אפשרות להפעיל את המערכת

download_template_file- הפונקצה הזה מאפשרת להוריד את קובץ השבלונה

download_maltego_zip-הפונקציה הזו מאפשרת את ההורדה של המלטגו הכולל בתוכו מדריך וקובץ ההגדרות

end_page- פונקציה זו בודקת אם הספגטי סיים לרוץ

running_flow- הפונקציה הזו מכילה ומריצה את סדר הפעולות של האתר

-	spaghetti.py
הסבר: קובץ זה מטפל בקישור בין המערכת למאגר המידע ויוצר מגעים ומאומתים עיקריים.

build_graph - ( פונקציה זו יוצרת אינדיבידואלים באמצעות מספר מזהה(פלאפון/ת’’ז/אפי

explore - פונקציה זו מוצאת חשיפות אפשריות לאישים המוכנסים למערכת ומחברת בהם במידה ויש התאמת זמני חשיפה.

fix_edges - פונקציה זו מגדירה חיבור בין אבא לבן, ומתקנת תקלה שהפכה את הבן לאבא, ולהפך.
summarize - פונקציה זו מוסיף שרשרת-זהות לאקסל, שרשרת זהות היא שילוב של ערך האש אשר מתקבל מהאובייקט, ומוסיפה לה מספרים אפים קשורים, פונקציית האש היא בלתי ניתנת להפיכה ומשומשת לצורכי הצפנה.

run_on_dataframe - פונקציה זו מטפלת בקישור של הדי-אף* שלנו והמערכת.
*DF - דאטה פריים

run_on_directory - פונקציה זו מריצה את הפונקציה העליונה (דאטה פריים) ושולחת את הנתונים שלנו למיקום הרצוי, שהוא תיקיית ספגטי גלובלית.

-	SQLConnector.py
הסבר: קובץ זה מטפל בתקשורת בין האתר לשרת הSQL ומאגר המידע המאוחסן בשרת.

-	queries.py
הסבר: קובץ זה מכיל את כל שאילתות הSQL הנדרשות למערכת ולמידע הרצוי.
split - פונקציה זו מקבלת רשימה ומחלקת אותה לחלקים בגודל מוגדר
build_select - פונקציה זו מקבלת רשימה של מספרים מזהים ומחזירה את הרשימה של המספרים ביחד עם יוניון אול* שמחבר אותם לצרכי אס-קיו-אל
* - יוניול אול - union all - מחבר רשימה של מספרים מזהים בsql.
get_patient_ids - פונקציה זו מקבלת את מספרי הזהות הנדרשים למטופל.
perform_exploration - פונקציה זו מוצאת מגעים.
map_to_variant_name - פונקציה זו ממפה את הוריאנטים על פי מדינות
get_patient_information - פונקציה זו מקבלת את הפרמטרים של ספגטי מהשרתי מידע של האס-קיו-אל

-	mail_sender.py
הסבר: קובץ זה לא בשימוש אך מכיל את האפשרות לקבל את תוצאות המערכת למייל.
attach_file - פונקציה זו מצרפת את הקובץ ספגטי להודעת אימייל
send_mail - פונקציה זו שולחת את המייל
-	enrichments.py
הסבר: קובץ זה מכיל פונקציות גנריות (מכיל פונקציה אחת שמיועדת להבדיל בין שתי עמודות)
hashfunc - פונקציה זו נועדה בשביל הצ’יין איי-די והיא נותנת את ערך ההאש של המאומת.