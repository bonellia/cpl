/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ceng.ceng351.bookdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


/**
 *
 * @author Kril
 */
public class BOOKDB implements IBOOKDB{
    private static Connection con = null;
    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your
     * implementation need initialization , necessary operations should be done
     * inside this function. For example, you can set your connection to the
     * database server inside this function.
     */
    /**
     * Place your initialization code inside if required.
     *
     * <p>
     * This function will be called before all other operations. If your
     * implementation need initialization , necessary operations should be done
     * inside this function. For example, you can set your connection to the
     * database server inside this function.
     */
    @Override
    public void initialize(){
        String url = "jdbc:mysql://144.122.71.65:8084/db1819325";

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            con = DriverManager.getConnection(url, "1819325", "f079c315");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace(System.err);
        }
    }

    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    @Override
    public int createTables(){
        con = null;
        Statement stm = null; 
        int result = 0;
        List<String> queryQueue = new ArrayList<>();
        
        // First, creating our query strings.
        // author(author_id:int, author_name:varchar(60))
        queryQueue.add(""
        + "CREATE TABLE author(author_id INT NOT NULL, "
        + "author_name VARCHAR(60), PRIMARY KEY (author_id))");
        
        // publisher(publisher_id:int, publisher_name:varchar(50))
        queryQueue.add(""
        + "CREATE TABLE publisher(publisher_id INT NOT NULL,"
        + "publisher_name VARCHAR(50), PRIMARY KEY (publisher_id))");
        
        // book(isbn:char(13), book_name:varchar(120), publisher_id:int,
        // first_publish_year:char(4), page_count:int, category:varchar(25),
        // rating:float)REFERENCES publisher(publisherid)
        queryQueue.add(""
        + "CREATE TABLE book(isbn CHAR(13) NOT NULL,"
        + "book_name VARCHAR(120) NOT NULL, publisher_id INT,"
        + "first_publish_year CHAR(4), page_count INT,"
        + "category VARCHAR(25), rating FLOAT,"
        + "PRIMARY KEY (isbn),"
        + "FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id)"
        + " ON UPDATE CASCADE ON DELETE CASCADE)");
        
        // author_of(isbn:char(13),author_id:int)REFERENCES book(isbn) author(authorid)
        queryQueue.add(""
        + "CREATE TABLE author_of(isbn CHAR(13) NOT NULL, author_id INT NOT NULL,"
        + "FOREIGN KEY (isbn) REFERENCES book(isbn)"
                    + " ON UPDATE CASCADE ON DELETE CASCADE,"
        + "FOREIGN KEY (author_id) REFERENCES author(author_id)"
                    + " ON UPDATE CASCADE ON DELETE CASCADE)");
        
        // phw1(isbn:char(13), book_name:varchar(120), rating:float)
        queryQueue.add(""
                + "CREATE TABLE phw1("
                + "isbn CHAR(13) NOT NULL,"
                + "book_name VARCHAR(120),"
                + "rating FLOAT)");
        for (String createTableQuery : queryQueue){
            try {
                // Establishing a new question for every query.
                // Might be inefficient, will visit later.
                initialize();
                if (con != null){
                    stm = con.createStatement();
                    result = stm.executeUpdate(createTableQuery);
                }
            }
            catch (SQLException e){
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (stm != null) stm.close();
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) con.close(); 
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }

    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    @Override
    public int dropTables(){
        // Need to drop tables with the correct order.
        // Otherwise REFERENCE constraint will be violated.
        con = null;
        Statement stm = null; 
        int result = 0;
        List<String> tableList = Arrays.asList("phw1", "author_of", "book", "publisher", "author");
        
        for (String tableName : tableList){
            String dropTableString = "DROP TABLE IF EXISTS " + tableName;
            try{
                initialize();
                if(con != null){
                stm = con.createStatement();
                result = stm.executeUpdate(dropTableString);
                con.close();
                }
            }
            catch (SQLException e) {
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (stm != null) {stm.close();}
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) {con.close();}
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }

    /**
     * Should insert an array of Author into the database.
     *
     * @param authors
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor(Author[] authors){
        con = null;
        int result = 0;
        PreparedStatement pstm = null;
        int authorCount = authors.length;
        for (int i = 0; i<authorCount; i++){
            try{
                if(con != null){
                    initialize();
                    pstm = con.prepareStatement("INSERT INTO book VALUES(?, ?)");
                    pstm.setInt(1, authors[i].getAuthor_id());
                    pstm.setString(2, authors[i].getAuthor_name().isEmpty() ? "NULL)" : authors[i].getAuthor_name());
                    result = pstm.executeUpdate();
                    con.close();
                }                
            }
            catch(SQLException e){
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (pstm != null) {pstm.close();}
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) {con.close();}
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }
    /**
     * Should insert an array of Book into the database.
     *
     * @param books
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertBook(Book[] books){
        con = null;
        int result = 0;
        PreparedStatement pstm = null;
        int bookCount = books.length;
        for (int i = 0; i<bookCount; i++){
            try{
                if (con != null){
                    initialize();
                    pstm = con.prepareStatement("INSERT INTO book VALUES(?, ?, ?, ?, ?, ?, ?)");
                    pstm.setString(1, books[i].getIsbn());
                    pstm.setString(2, books[i].getBook_name().isEmpty() ? "NULL)" : books[i].getBook_name());
                    pstm.setInt(3, books[i].getPublisher_id());
                    pstm.setString(4, books[i].getFirst_publish_year().isEmpty() ? "NULL)" : books[i].getFirst_publish_year());
                    pstm.setInt(5, books[i].getPage_count());
                    pstm.setString(6, books[i].getCategory().isEmpty() ? "NULL)" : books[i].getCategory());
                    pstm.setDouble(7, books[i].getRating());
                    result = pstm.executeUpdate();
                    con.close();
                }
            }
            catch(SQLException e){
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (pstm != null) {pstm.close();}
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) {con.close();}
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }

    /**
     * Should insert an array of Publisher into the database.
     *
     * @param publishers
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertPublisher(Publisher[] publishers){
        con = null;
        int result = 0;
        PreparedStatement pstm = null;
        int publisherCount = publishers.length;
        for (int i = 0; i<publisherCount; i++){
            try{
                if (con != null){
                    initialize();
                    pstm = con.prepareStatement("INSERT INTO book VALUES(?, ?)");
                    pstm.setInt(1, publishers[i].getPublisher_id());
                    pstm.setString(2, publishers[i].getPublisher_name().isEmpty() ? "NULL)" : publishers[i].getPublisher_name());
                    result = pstm.executeUpdate();
                    con.close();
                }                
            }
            catch(SQLException e){
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (pstm != null) {pstm.close();}
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) {con.close();}
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }

    /**
     * Should insert an array of Author_of into the database.
     *
     * @param author_ofs
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor_of(Author_of[] author_ofs){
        con = null;
        int result = 0;
        PreparedStatement pstm = null;
        int authorOfCount = author_ofs.length;
        for (int i = 0; i<authorOfCount; i++){
            try{
                if (con != null){
                    initialize();
                    pstm = con.prepareStatement("INSERT INTO book VALUES(?, ?)");
                    pstm.setString(1, author_ofs[i].getIsbn().isEmpty() ? "NULL)" : author_ofs[i].getIsbn());
                    pstm.setInt(2, author_ofs[i].getAuthor_id());
                    result = pstm.executeUpdate();
                    con.close();
                }
                
            }
            catch(SQLException e){
                e.printStackTrace(System.err);
            }
            finally {
                try {
                    if (pstm != null) {pstm.close();}
                }
                catch (SQLException e) { 
                    e.printStackTrace(System.err);
                }
                try {
                    if (con != null) {con.close();}
                }
                catch (SQLException e) {
                    e.printStackTrace(System.err);
                }
            }
        }
        return result;
    }

    /**
     * Should return isbn, first_publish_year, page_count and publisher_name of 
     * the books which have the maximum number of pages.
     * @return QueryResult.ResultQ1[]
     */
    @Override
    public QueryResult.ResultQ1[] functionQ1(){
        QueryResult.ResultQ1[] result = null;
        return result;
    }
    
    /**
     * For the publishers who have published books that were co-authored by both 
     * of the given authors(author1 and author2); list publisher_id(s) and average
     * page_count(s)  of all the books these publishers have published.
     * @param author_id1
     * @param author_id2
     * @return QueryResult.ResultQ2[]
     */

    @Override
    public QueryResult.ResultQ2[] functionQ2(int author_id1, int author_id2){
        QueryResult.ResultQ2[] result = null;
        return result;
    }
    
    /**
     * List book_name, category and first_publish_year of the earliest 
     * published book(s) of the author(s) whose author_name is given.
     * @param author_name
     * @return QueryResult.ResultQ3[]
     */

    @Override
    public QueryResult.ResultQ3[] functionQ3(String author_name){
        QueryResult.ResultQ3[] result = null;
        return result;
    }

    /**
     * For publishers whose name contains at least 3 words 
     * (i.e., "Koc Universitesi Yayinlari"), have published at least 3 books 
     * and average rating of their books are greater than(>) 3; 
     * list their publisher_id(s) and distinct category(ies) they have published. 
     * PS: You may assume that each word in publisher_name is seperated by a space.
     * @return QueryResult.ResultQ4[]
     */
    @Override
    public QueryResult.ResultQ4[] functionQ4(){
        QueryResult.ResultQ4[] result = null;
        return result;
    }
    
    /**
     * List author_id and author_name of the authors who have worked with 
     * all the publishers that the given author_id has worked.
     * @param author_id
     * @return QueryResult.ResultQ5[]
     */

    @Override
    public QueryResult.ResultQ5[] functionQ5(int author_id){
        QueryResult.ResultQ5[] result = null;
        return result;
    }
    
    /**
     * List author_name(s) and isbn(s) of the book(s) written by "Selective" authors. 
     * "Selective" authors are those who have worked with publishers that have 
     * published their books only.(i.e., they haven't published books of 
     * different authors)
     * @return QueryResult.ResultQ6[]
     */

    @Override
    public QueryResult.ResultQ6[] functionQ6(){
        QueryResult.ResultQ6[] result = null;
        return result;
    }

    /**
     * List publisher_id and publisher_name of the publishers who have published 
     * at least 2 books in  'Roman' category and average rating of their books 
     * are more than (>) the given value.
     * @param rating
     * @return QueryResult.ResultQ7[]
     */
    @Override
    public QueryResult.ResultQ7[] functionQ7(double rating){
        QueryResult.ResultQ7[] result = null;
        return result;
    }
    
    /**
     * Some of the books  in the store have been published more than once: 
     * although they have same names(book\_name), they are published with different
     * isbns. For each  multiple copy of these books, find the book_name(s) with the 
     * lowest rating for each book_name  and store their isbn, book_name and 
     * rating into phw1 table using a single BULK insertion query. 
     * If there exists more than 1 with the lowest rating, then store them all.
     * After the bulk insertion operation, list isbn, book_name and rating of 
     * all rows in phw1 table.
     * @return QueryResult.ResultQ8[]
     */

    @Override
    public QueryResult.ResultQ8[] functionQ8(){
        QueryResult.ResultQ8[] result = null;
        return result;
    }
    
    /**
     * For the books that contain the given keyword anywhere in their names, 
     * increase their ratings by one. 
     * Please note that, the maximum rating cannot be more than 5, 
     * therefore if the previous rating is greater than 4, do not update the 
     * rating of that book. 
     * @param keyword
     * @return sum of the ratings of all books
     */

    @Override
    public double functionQ9(String keyword){
        return 0;
    }
    
    /**
     * Delete publishers in publisher table who haven't published a book yet. 
     * @return count of all rows of the publisher table after delete operation.
     */
    @Override
    public int function10(){
        return 0;
    }
}
