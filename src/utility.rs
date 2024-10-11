use time::{macros::format_description, Date, PrimitiveDateTime, Time};

pub fn parse_to_primitive_datetime(date_string: &str) -> Result<PrimitiveDateTime, String> {
    // Define the possible date formats using the `format_description!` macro
    let date_formats = [
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond]"), // e.g., 2024-06-26 02:06:05.123456
        format_description!("[year]-[month]-[day] [hour]:[minute]:[second]"), // e.g., 2024-06-26 02:06:05
    ];

    // Try each format
    for format in date_formats {
        match PrimitiveDateTime::parse(date_string, format) {
            Ok(datetime) => return Ok(datetime),
            Err(_) => continue, // Try the next format
        }
    }

    // If no format matches, try parsing as a date-only string
    match Date::parse(date_string,  format_description!("[year]-[month]-[day]")){
        Ok(date) => {
            // Combine the parsed date with a default time component
            let default_time = Time::MIDNIGHT;
            return Ok(date.with_time(default_time))
        }
        Err(_) => return Err("Invalid date format".to_string()),
    }
}
