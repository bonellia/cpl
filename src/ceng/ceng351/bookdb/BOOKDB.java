/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ceng.ceng351.bookdb;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;


/**
 *
 * @author Kril
 */
public class BOOKDB implements IBOOKDB{
    
    private static Connection con;
    
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
    }

    /**
     * Should create the necessary tables when called.
     *
     * @return the number of tables that are created successfully.
     */
    @Override
    public int createTables(){
        
        int result;
        Statement stm;
        
        // First, creating our query strings.
        // author(author_id:int, author_name:varchar(60))
        String queryCreateAuthorTable = ""
                + "CREATE TABLE author("
                + "author_id INT NOT NULL,"
                + "author_name VARCHAR(60),"
                + "PRIMARY KEY (author_id))";
        try {
            stm = BOOKDB.con.createStatement();
            result = stm.executeUpdate(queryCreateAuthorTable);
            stm.close();
        }
        catch (SQLException e){
            System.out.println("Failed to create Author table with the exception "+e);
        }
        // publisher(publisher_id:int, publisher_name:varchar(50))
        String queryCreatePublisherTable = ""
                + "CREATE TABLE publisher("
                + "publisher_id INT NOT NULL,"
                + "publisher_name VARCHAR(50),"
                + "PRIMARY KEY (publisherid))";
        try {
            stm = BOOKDB.con.createStatement();
            result = stm.executeUpdate(queryCreatePublisherTable);
            stm.close();
        }
        catch (SQLException e){
            System.out.println("Failed to create publisher table with the exception "+e);
        }
        // book(isbn:char(13), book_name:varchar(120), publisher_id:int,
        // first_publish_year:char(4), page_count:int, category:varchar(25),
        // rating:float)REFERENCES publisher(publisherid)
        String queryCreateBookTable = ""
                + "CREATE TABLE book("
                + "isbn CHAR(13) NOT NULL,"
                + "book_name VARCHAR(120) NOT NULL,"
                + "pubblisher_id INT,"
                + "first_publish_year CHAR(4),"
                + "page_count INT,"
                + "category VARCHAR(25),"
                + "rating FLOAT,"
                + "PRIMARY KEY (isbn),"
                + "FOREIGN KEY (publisher_id) REFERENCES publisher(publisher_id)"
                            + " ON UPDATE CASCADE ON DELETE CASCADE)";
        try {
            stm = BOOKDB.con.createStatement();
            result = stm.executeUpdate(queryCreateBookTable);
            stm.close();
        }
        catch (SQLException e){
            System.out.println("Failed to create book table with the exception "+e);
        }
        // author_of(isbn:char(13),author_id:int)REFERENCES book(isbn) author(authorid)
        String queryCreateAuthorOfTable = ""
                + "CREATE TABLE author_of("
                + "isbn CHAR(13) NOT NULL,"
                + "author_id INT NOT NULL,"
                + "FOREIGN KEY (isbn) REFERENCES book(isbn)"
                            + " ON UPDATE CASCADE ON DELETE CASCADE,"
                + "FOREIGN KEY (author_id) REFERENCES author(author_id)"
                            + " ON UPDATE CASCADE ON DELETE CASCADE)";
        try {
            stm = BOOKDB.con.createStatement();
            result = stm.executeUpdate(queryCreateAuthorOfTable);
            stm.close();
        }
        catch (SQLException e){
            System.out.println("Failed to create author_of table with the exception "+e);
        }
        // phw1(isbn:char(13), book_name:varchar(120), rating:float)
        String queryCreatePhw1Table = ""
                + "CREATE TABLE phw1("
                + "isbn CHAR(13) NOT NULL,"
                + "book_name VARCHAR(120),"
                + "rating FLOAT)";
        try {
            stm = BOOKDB.con.createStatement();
            result = stm.executeUpdate(queryCreatePhw1Table);
            stm.close();
        }
        catch (SQLException e){
            System.out.println("Failed to create phw1 table with the exception "+e);
        }
        return 0;
    }

    /**
     * Should drop the tables if exists when called.
     *
     * @return the number of tables are dropped successfully.
     */
    @Override
    public int dropTables(){
        return 0;
    }

    /**
     * Should insert an array of Author into the database.
     *
     * @param authors
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor(Author[] authors){
        return 0;
    }

    /**
     * Should insert an array of Book into the database.
     *
     * @param books
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertBook(Book[] books){
        return 0;
    }

    /**
     * Should insert an array of Publisher into the database.
     *
     * @param publishers
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertPublisher(Publisher[] publishers){
        return 0;
    }

    /**
     * Should insert an array of Author_of into the database.
     *
     * @param author_ofs
     * @return Number of rows inserted successfully.
     */
    @Override
    public int insertAuthor_of(Author_of[] author_ofs){
        return 0;
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
