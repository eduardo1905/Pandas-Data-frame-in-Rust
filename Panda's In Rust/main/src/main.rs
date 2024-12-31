use std::error::Error;
use std::fmt;
use std::process;

// Different types that the table may have
#[derive(Debug, Clone)]
enum ColumnVal {
    One(String),
    Two(bool),
    Three(f64),
    Four(i64),
}

#[derive(Debug)]
struct DataFrame {
    // 3 types that any CSV table will contain
    labels: Vec<String>,
    types: Vec<u32>,
    rows: Vec<Vec<ColumnVal>>, // Whatever the row value is the table will reflect it
}

// For returning errors
#[derive(Debug)]
struct MyError(String);

impl fmt::Display for MyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "There is an error: {}", self.0)
    }
}
impl Error for MyError {}

impl DataFrame {
    // Creates an empty dataframe
    fn new() -> Self {

        let data_labels: Vec<String> = Vec::new();
        let types: Vec<u32> = Vec::new();
        let data_columns: Vec<Vec<ColumnVal>> = Vec::new();

        let dataframe = DataFrame {
            labels: data_labels,
            types: types,
            rows: data_columns,
        };

        return dataframe
    }

    // Reads a given csv file and implements it in the empty dataframe from new 
    fn read_csv(&mut self, path: &str, types: &Vec<u32>) -> Result<(), Box<dyn Error>> {
        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b',')
            .has_headers(false)
            .flexible(true)
            .from_path(path)?;
        let mut first_row = true;
        for result in rdr.records() {
            // Notice that we need to provide a type hint for automatic
            // deserialization.
            let r = result.unwrap();
            let mut row: Vec<ColumnVal> = vec![];
            if first_row {
                for elem in r.iter() {
                    // These are the labels
                    self.labels.push(elem.to_string())
                }
                first_row = false;
                continue;
            }
            for (i, elem) in r.iter().enumerate() {
                match types[i] {

                    1 => row.push(ColumnVal::One(elem.to_string())),
                    2 => row.push(ColumnVal::Two(elem.parse::<i64>().unwrap() != 0)),
                    3 => row.push(ColumnVal::Three(elem.parse::<f64>().unwrap())),
                    4 => row.push(ColumnVal::Four(elem.parse::<i64>().unwrap())),
                    _ => return Err(Box::new(MyError("Unknown type".to_string()))),
                }
            }
            // Put the data into the dataframe
            self.rows.push(row);
        }
        self.types = types.clone();
        Ok(())
    }

    fn unimplemented() {}

    // Prints out the data frame to look nice
    fn print(&self) {
        // print the labels
        for label in &self.labels {
            print!("{:?}",label);
        }
        // print the data
        for row in &self.rows {
            for column_value in row {
                match column_value {
                    ColumnVal::One(val) => { print!("{:<15}", val); },
                    ColumnVal::Two(val) => { print!("{:<15}", val); }, 
                    ColumnVal::Three(val) => { print!("{:<15.0}", val);},
                    ColumnVal::Four(val) => { print!("{:<15}", val); },
                }
            }
            println!();
        }
    }


    // Allows for a collumn of values to be added to the dataframe if the length of new values matches the length of current table values
    fn add_column(&mut self, label:&str, dtype:u32, vals:&Vec<ColumnVal>) -> Result<(), Box<dyn Error>> {

        if self.rows.len() == vals.len() {
            // Iterator and closure to add a column by iterating through both self.rows and vals
            self.rows.iter_mut().zip(vals.iter()).for_each(|(row, val)| row.push(val.clone()));
            self.labels.push(label.to_string());
            self.types.push(dtype);
            Ok(())
        }
    
        else {
            Err(Box::from("Length does not match"))
        }
    }
    
    // Allows for a fusion of two tables
    fn merge_frame(&mut self, dataframe: DataFrame) -> Result<(), Box<dyn Error>> {
        //unimplemented!();
        if self.types == dataframe.types {
            self.rows.extend(dataframe.rows);
            Ok(())
        }
        else {
            Err(Box::from("Type does not match"))
        }
    }

    // Gets specific columns from the created data frame
    fn find_columns(&mut self, labels: Vec<String>) -> Result<Vec<usize>, Box<dyn Error>> {
        let mut label_index = Vec::new();
        
        // Iterate over self.labels and check if each label is in the labels argument
        self.labels.iter().enumerate().for_each(|(i, label)| {
            if labels.contains(&label) {
                label_index.push(i);
            }

        });
    
        Ok(label_index)
    }
       

    // Prints out selected columns from find_columns
    fn restrict_columns(&mut self, labels: Vec<String>) -> Result<DataFrame, Box<dyn Error>> {
        let data_find = self.find_columns(labels.clone())?; // Get indices of matching columns
        
        // Create a new DataFrame
        let mut new_dataframe = DataFrame::new();
    
        if !data_find.is_empty() {
            // Initialize rows for the new DataFrame
            new_dataframe.rows = vec![vec![]; self.rows.len()];
    
            // Add the selected columns to the new DataFrame
            for &col_idx in &data_find {
                let label = &self.labels[col_idx];
                let dtype = self.types[col_idx];
                let column_data: Vec<ColumnVal> = self.rows.iter().map(|row| row[col_idx].clone()).collect();
                new_dataframe.add_column(label, dtype, &column_data)?; // Add the column
            }
        }
    
        Ok(new_dataframe)
    }
    
    // Filters the dataframe utilizing a closure 
    fn filter(
        &mut self,
        label: &str,
        operation: fn(&ColumnVal) -> bool,
    ) -> Result<Self, Box<dyn Error>> {
        
        let mut included_label = Vec::new();
        let mut included_row = Vec::new();

        included_label.push(label.to_string());
        let data = self.find_columns(included_label).unwrap();
        for i in &self.rows {
            if operation(&i[data[0]]) {
                included_row.push(i.clone());
            }
        }
        self.rows = included_row.clone();
        Ok(DataFrame {
            labels: self.labels.clone(),
            types: self.types.clone(),
            rows: included_row.clone(),
        })
    }

    // Finds all data in the given column and puts it into a vector 

    fn column_op(&self, labels: &[String]) -> Result<Vec<f64>, Box<dyn Error>> {
        // Find the indices of the requested columns using `self.labels` instead of `self.headers`
        let indices: Result<Vec<usize>, Box<dyn Error>> = labels
            .iter()
            .map(|label| {
                self.labels
                    .iter()
                    .position(|header| header == label)
                    .ok_or_else(|| Box::new(MyError(format!("column '{}' doesn't exist", label))) as Box<dyn Error>)
            })
            .collect();
    
        let indices = indices?; // Unwrap the Result<Vec<usize>, _>
    
        let mut numeric_values = Vec::new();
    
        // Iterate over the indices and rows to collect numeric values
        for &index in &indices {
            for row in &self.rows {
                match &row[index] {
                    ColumnVal::Three(num) => numeric_values.push(*num),
                    ColumnVal::Four(num) => numeric_values.push(*num as f64),
                    _ => return Err(Box::new(MyError("Numeric data not found".to_string()))),
                }
            }
        }
    
        Ok(numeric_values)
    }
    
    
     // Calculate the avrage of a column
     fn average(&mut self, label: &str) -> Result<f64, Box<dyn std::error::Error>> {
        // Retrieve the column values for the given label
        let column_values = self.column_op(&[label.to_string()])?;
    
        // Check if the column is empty
        if column_values.is_empty() {
            return Err(Box::new(MyError("No data found".to_string())));
        }
    
        // Calculate the sum and average
        let sum: f64 = column_values.iter().sum();
        Ok(sum / column_values.len() as f64)

    }
     

    // calculates the summation of 2 rows
    fn add_rows(&self, col1: &str, col2: &str) -> Result<Vec<f64>, Box<dyn std::error::Error>> {
        // Retrieve values for both columns
        let column1_val = self.column_op(&[col1.to_string()])?;
        let column2_val = self.column_op(&[col2.to_string()])?;
    
        // Check if the lengths of the columns match
        if column1_val.len() != column2_val.len() {
            return Err(Box::new(MyError("Lengths don't match".to_string())));
        }
    
        // Add corresponding values from both columns
        let added_row: Vec<f64> = column1_val
            .iter()
            .zip(column2_val.iter())
            .map(|(val1, val2)| val1 + val2)
            .collect();
    
        Ok(added_row) // Return the result
    }
}


fn main() -> Result<(), Box<dyn Error>> {
    //printing the original data frmae
    let mut dataframe = DataFrame::new();
    let types = vec![1, 4, 3, 4, 4, 2];
    dataframe.read_csv("pizza.csv", &types)?; //read it
    println!("Original dataframe:");
    dataframe.print(); // print

    //added column
    let new_column_vals = vec![
        ColumnVal::Four(101),  // TeamID or similar data
        ColumnVal::Four(102),
        ColumnVal::Four(103),
        ColumnVal::Four(104),
        ColumnVal::Four(105),
    ]; 
    dataframe.add_column("TeamID",4, &new_column_vals)?;
    //Adds the new column
    dataframe.print();

    //merged frames
    println!("");
    println!("");
    let mut dataframe_og = DataFrame::new();
    let mut dataframe2 = DataFrame::new();
    dataframe_og.read_csv("pizza.csv", &types)?;
    dataframe2.read_csv("pizza2.csv", &types)?;
    dataframe_og.merge_frame(dataframe2)?;
    println!("Merged DataFrame:"); // Print the merged DataFrame
    dataframe_og.print();

     //select columns
    println!("");
    println!("");
    let mut select_df = DataFrame::new();
    select_df.read_csv("pizza.csv", &types)?;
    let selected_df =select_df.restrict_columns(vec!["Name".to_string(),"Number".to_string()])?;
    println!("Selected Columns DataFrame:");
    selected_df.print();

    //filitered df
    println!("");
    println!("");
    let mut unfiltered_df = DataFrame::new();
    unfiltered_df.read_csv("pizza.csv", &types)?; //make unfitierd df pizza
    let filtered_df = unfiltered_df.filter("LikesPizza", |val| {
    //filter by pizza column where values are true
    matches!(val, ColumnVal::Two(true))})?;
    println!("Filtered DataFrame:");
    filtered_df.print();

    // column op
    let mut df = DataFrame::new();
    df.read_csv("pizza.csv", &types)?;
    let ppg_column = df.column_op(&["PPG".to_string()])?;
    println!("PPG Column: {:?}", ppg_column); // Should print: [24.6, 25.0, 27.0, 25.0, 30.1]

    // Test 2: average - Compute the average PPG
    let avg_ppg = df.average("PPG")?;
    println!("Average PPG: {:?}", avg_ppg); // Should print: ~26.14

    // Test 3: add_rows - Add the "PPG" and "TotalPoints" columns row-wise
    let added_rows = df.add_rows("PPG", "TotalPoints")?;
    println!("Added Rows (PPG + TotalPoints): {:?}", added_rows);
    // Expected output something like: [38311.6, 36953.0, 36408.0, 33668.0, 32322.1]

    Ok(())
}

 
