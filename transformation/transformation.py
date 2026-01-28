import pandas as pd
import re
import html


INPUT_CSV = "data/processed/cleaned_RC_Book.csv"
OUTPUT_CSV = "data/processed/cleaned_RC_Book1.csv"

drop_col = [
    'Unnamed: 0.' , 'Unnamed: 0.1' , 'Unnamed: 10',
    'Unnamed: 11', 'Unnamed: 12', 'Unnamed: 13',
    'Unnamed: 14', 'Unnamed: 15', 'Unnamed: 16',
    'Unnamed: 17', 'Unnamed: 18', 'Unnamed: 19',
    'Unnamed: 20' , 'Year'
]


## Load Data 

def load_data(INPUT_CSV):
    print(f"Loading data from {INPUT_CSV}")
    return pd.read_csv(INPUT_CSV, encoding="latin-1", low_memory=False)

## Drop Columns

def drop_columns(df , columns):
    df = df.drop(columns = columns , errors = "ignore")
    return df


## Clean Description Part(Remove HTML tags , HTML entities)

def clean_description(text):
    if not text or pd.isna(text):
        return None

    text = str(text)
    text = html.unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-zA-Z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)

    if len(text) <= 10:
        print("Description not Available")

    return text.strip()

## Clean Author Name(Normalize the Name {Remove ,.!})

def clean_author(author):
    if not author or pd.isna(author):
        return None

    author = str(author).lower()
    # remove unwanted punctuation
    author = re.sub(r"[.,/]", " ", author)
    # remove brackets content
    author = re.sub(r"\(.*?\)", "", author)
    # remove numbers
    author = re.sub(r"\d+", "", author)
    # keep letters and spaces only
    author = re.sub(r"[^a-z ]", " ", author)
    # normalize spaces
    author = re.sub(r"\s+", " ", author)

    return author.strip()

## Formatting the column Names

def Format_col(df):
    df = df.rename(columns={
    'Author/Editor' : "Author",
    'Class No./Book No.' : "Class_no",
    'Page(s)' : "pages",
    'Acc. No.' : "Acc_No",
    'Acc. Date' : "Acc_Date",
    "Place & Publisher" : "Place_&_Publisher"
    })

    return df
    

def transformation():
    df = load_data(INPUT_CSV)
    df['book_description'] = df['book_description'].apply(clean_description)
    df["Author"] = df["Author"].apply(clean_author)
    df = drop_columns(df,drop_col)
    df = Format_col(df)

    df.to_csv(OUTPUT_CSV, index=False)
    print("Transformation completed successfully ✅")
 
if __name__ == "__main__":
    transformation()


