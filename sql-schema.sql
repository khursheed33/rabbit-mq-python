-- Create Messages Table
CREATE TABLE Messages (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Content NVARCHAR(MAX) NOT NULL,
    UserName NVARCHAR(100) NOT NULL,
    Timestamp DATETIME2 DEFAULT GETDATE(),
    INDEX IX_Messages_Timestamp (Timestamp DESC)
);

-- Create stored procedure for message insertion
CREATE PROCEDURE sp_InsertMessage
    @Content NVARCHAR(MAX),
    @UserName NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    INSERT INTO Messages (Content, UserName)
    VALUES (@Content, @UserName);
    
    SELECT SCOPE_IDENTITY() as MessageId;
END;

-- Create stored procedure for retrieving message history
CREATE PROCEDURE sp_GetMessageHistory
    @Limit INT = 50
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT TOP (@Limit)
        Id,
        Content,
        UserName,
        Timestamp
    FROM Messages
    ORDER BY Timestamp DESC;
END;

-- Create index for better query performance
CREATE NONCLUSTERED INDEX IX_Messages_UserName
ON Messages(UserName);
