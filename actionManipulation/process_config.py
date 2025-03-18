''' 
    CREATE TABLE Process_Config_dt (
    Process_Config_dt_id INT PRIMARY KEY IDENTITY(1,1),  -- Unique identifier for the detail record (auto-increment)
    Process_Config_id INT NOT NULL,                      -- Foreign key referencing Process_Config_hd
    Process_Config_param INT NOT NULL,                   -- Parameter value (e.g., time in hours)
    end_dat DATETIME,                                    -- Timestamp when the parameter ends (optional)
    CONSTRAINT FK_Process_Config_hd FOREIGN KEY (Process_Config_id) 
        REFERENCES Process_Config_hd(Process_Config_id)  -- Foreign key constraint
);
    
'''

# Import the necessary libraries
