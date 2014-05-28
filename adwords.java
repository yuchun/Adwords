//package adwords;
/*s sample shows how to list all the names from the EMP table
 *
 * It uses the JDBC THIN driver.  See the same program in the
 * oci8 samples directory to see how to use the other drivers.
 */

// You need to import the java.sql package to use JDBC
import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.regex.*;

class adwords
{
	public static void main (String args [])throws SQLException
	{
		adwords ad = new adwords();
		ad.start();
		ad.greedyFirst();
		ad.balanceFirst();
		ad.generalBalanceFirst();
		ad.greedySecond();
		ad.balanceSecond();
		ad.generalBalanceSecond();
		ad.finish();
	}
	public adwords()
	{
		numAds = new int[6];
		outputWriter = new PrintWriter[6];
	}
	
	public void start() throws SQLException
	{
		parseSystem();
		// Load the Oracle JDBC driver
		DriverManager.registerDriver(new oracle.jdbc.OracleDriver());
		conn = 	DriverManager.getConnection ("jdbc:oracle:thin:hr/hr@oracle1.cise.ufl.edu:1521:orcl", getUsername(), getPassword());
		stmt = conn.createStatement ();
		
		clearTables();
		createTempTables();
		createAdvertisersTable();	
		createKeywordsTable();
		createQueriesTable();
		
		upProcGetQuery();
		
		upProcGeedyFirst();
		upProcGeedySecond();
		upProcBalanceFirst();
		upProcBalanceSecond();
		upProcGeneralBalanceFirst();
		upProcGeneralBalanceSecond();

		try{
			for(int i = 0; i < outputWriter.length; i++)
				outputWriter[i] = new PrintWriter(NameOutput + "." + (i + 1)); 
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}

	}

	public void finish() throws SQLException
	{
		for(int i = 0; i < outputWriter.length; i++)
			outputWriter[i].close();
		conn.close(); // ** IMPORTANT : Close connections when done **
		System.out.println("Good bye");
	}

	public void greedyFirst() throws SQLException
	{
		System.out.print("1. Process first price auction and the greedy algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doGreedyFirst(?)}");
		cstmt.setInt(1, numAds[0]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[0].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[0].flush();
		rs.close();
		System.out.println("done");
	}
	
	public void greedySecond() throws SQLException
	{
		System.out.print("4. Process second price auction and the greedy algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doGreedySecond(?)}");
		cstmt.setInt(1, numAds[3]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[1].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[1].flush();
		rs.close();
		System.out.println("done");
	}

	public void balanceFirst() throws SQLException
	{
		System.out.print("2. Process first price auction and the balance algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doBalanceFirst(?)}");
		cstmt.setInt(1, numAds[1]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[2].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[2].flush();
		rs.close();
		System.out.println("done");
	}


	public void balanceSecond() throws SQLException
	{
		System.out.print("5. Process second price auction and the balance algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doBalanceSecond(?)}");
		cstmt.setInt(1, numAds[4]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[3].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[3].flush();
		rs.close();
		System.out.println("done");
	}

	public void generalBalanceFirst() throws SQLException
	{
		System.out.print("3. Process first price auction and the general balance algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doGeneralBalanceFirst(?)}");
		cstmt.setInt(1, numAds[2]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[4].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[4].flush();
		rs.close();
		System.out.println("done");
	}

	
	public void generalBalanceSecond() throws SQLException
	{
		System.out.print("6. Process second price auction and the general balance algorithm. Please wait...");
		restoreAdvertisers();
		CallableStatement cstmt = conn.prepareCall("{call doGeneralBalanceSecond(?)}");
		cstmt.setInt(1, numAds[5]);
		cstmt.executeUpdate();
		conn.commit();
		cstmt.close();
		String sqlQuery = "SELECT * FROM Output ORDER BY qId, rank";
		ResultSet rs = stmt.executeQuery(sqlQuery);
		while(rs.next()){
			outputWriter[5].println(rs.getInt(1) + ", " + rs.getInt(2) + ", " + rs.getInt(3) + ", " + rs.getFloat(4) + ", " + rs.getFloat(5));

		}
		outputWriter[5].flush();
		rs.close();
		System.out.println("done");
	}

	private void upProcGeedyFirst() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doGreedyFirst(num_ads IN INTEGER) \n" +
			" IS \n" +
			" num_queries INTEGER; \n" +
			" queryId INTEGER; \n" +
			" loopq INTEGER; \n" +
			" loopc INTEGER; \n" +
			" oldbidt INTEGER; \n" +
			" oldctc FLOAT; \n" +
			" newbalance FLOAT; \n" +
			" tempbudget FLOAT; \n" +
			" cur_greedy TBid%rowtype; \n" +
			" rec_greedy TBid%rowtype; \n" +
			" BEGIN \n" +
			" 	DELETE Output; \n" +
			"	SELECT COUNT(*) \n" +
			"	INTO num_queries \n" +
			"	FROM Queries; \n" +
			"	FOR loopq IN 1..num_queries \n" +
			"	LOOP \n" +
			"		DELETE Tquery; \n" +
			"		DELETE TempQ; \n" +
			"		DELETE TBid; \n" +
			"		getQuery(loopq); \n" +		
			"		SELECT qId INTO queryId FROM TQuery WHERE rownum = 1; \n" +
			"		commit work; \n" +
			"		loopc := 0; \n" +
			"		FOR cur_greedy IN ( \n" +
			"		Select advertiserid, ROUND(ctc*similarity.cosine*totalbids, 9) as adrank, totalbids \n" +
			"		FROM( \n" +
			"			Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
			"	 		FROM \n" +
			"			((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
			"			From (Select advertiserId, count(keyword) as cnt \n" +
			"			From Keywords \n" +
			"			WHERE advertiserId IN (SELECT advertiserId \n" +
			"							       FROM Keywords \n" +
			"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
			"			Group by advertiserId,keyword order by advertiserid) Temp \n" +
			"			Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
			"			From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
			"			From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
			"			From TQuery q, Keywords k \n" +
			"			Where k.keyword = q.word ) KWDSTAT \n" +
			"			Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
			"		WHERE totalbids <= advertisers.balance \n" +
			"		ORDER BY adrank DESC, advertiserId ASC) \n" +
			"		LOOP \n" +
			"			loopc := loopc + 1; \n" +
			"			EXIT WHEN loopc > num_ads; \n" +	
			"			SELECT balance, budget, bidtime, ctc  \n" +
			"			INTO newbalance, tempbudget, oldbidt, oldctc \n" +
			"			FROM Advertisers \n" +
			"			WHERE Advertisers.advertiserId = cur_greedy.advertiserId; \n" +
			"			IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
			"			THEN \n" +
			"				newbalance := ROUND(newbalance - cur_greedy.totalbids, 2); \n" +
			"			END IF; \n" +
			"			INSERT INTO Output  \n" +
			"			VALUES(queryId, loopc, cur_greedy.advertiserId, newbalance, tempbudget); \n" +
			"			UPDATE Advertisers \n" +
			"			SET balance = newbalance, bidtime = oldbidt + 1 \n" +
			"			WHERE advertiserId = cur_greedy.advertiserId; \n" +
			"		END LOOP; \n" +
			"		commit work;			 \n" +
			"	END LOOP;	 \n" +
			" END; \n";
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}

	private void upProcBalanceFirst() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doBalanceFirst(num_ads IN INTEGER) \n" +
				" IS \n" +
				" num_queries INTEGER; \n" +
				" queryId INTEGER; \n" +
				" loopq INTEGER; \n" +
				" loopc INTEGER; \n" +
				" oldbidt INTEGER; \n" +
				" oldctc FLOAT; \n" +
				" newbalance FLOAT; \n" +
				" tempbudget FLOAT; \n" +
				" cur_greedy TBid%rowtype; \n" +
				" rec_greedy TBid%rowtype; \n" +
				" BEGIN \n" +
				" 	DELETE Output; \n" +
				"	SELECT COUNT(*) \n" +
				"	INTO num_queries \n" +
				"	FROM Queries; \n" +
				"	FOR loopq IN 1..num_queries \n" +
				"	LOOP \n" +
				"		DELETE Tquery; \n" +
				"		DELETE TempQ; \n" +
				"		DELETE TBid; \n" +
				"		getQuery(loopq); \n" +		
				"		SELECT qId INTO queryId FROM TQuery WHERE rownum = 1; \n" +
				"		commit work; \n" +
				"		loopc := 0; \n" +
				"		FOR cur_greedy IN ( \n" +
				"		Select advertiserid, ROUND(ctc*similarity.cosine* balance, 9) as adrank, totalbids  \n" +
				"		FROM( \n" +
				"			Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
				"	 		FROM \n" +
				"			((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
				"			From (Select advertiserId, count(keyword) as cnt \n" +
				"			From Keywords \n" +
				"			WHERE advertiserId IN (SELECT advertiserId \n" +
				"							       FROM Keywords \n" +
				"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
				"			Group by advertiserId,keyword order by advertiserid) Temp \n" +
				"			Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
				"			From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
				"			From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
				"			From TQuery q, Keywords k \n" +
				"			Where k.keyword = q.word ) KWDSTAT \n" +
				"			Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
				"		WHERE totalbids <= advertisers.balance \n" +
				"		ORDER BY adrank DESC, advertiserId ASC) \n" +
				"		LOOP \n" +
				"			loopc := loopc + 1; \n" +
				"			EXIT WHEN loopc > num_ads; \n" +	
				"			SELECT balance, budget, bidtime, ctc  \n" +
				"			INTO newbalance, tempbudget, oldbidt, oldctc \n" +
				"			FROM Advertisers \n" +
				"			WHERE Advertisers.advertiserId = cur_greedy.advertiserId; \n" +
				"			IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
				"			THEN \n" +
				"				newbalance := ROUND(newbalance - cur_greedy.totalbids, 2); \n" +
				"			END IF; \n" +
				"			INSERT INTO Output  \n" +
				"			VALUES(queryId, loopc, cur_greedy.advertiserId, newbalance, tempbudget); \n" +
				"			UPDATE Advertisers \n" +
				"			SET balance = newbalance, bidtime = oldbidt + 1 \n" +
				"			WHERE advertiserId = cur_greedy.advertiserId; \n" +
				"		END LOOP; \n" +
				"		commit work;			 \n" +
				"	END LOOP;	 \n" +
				" END; \n";
		
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}
	
	private void upProcGeneralBalanceFirst() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doGeneralBalanceFirst(num_ads IN INTEGER) \n" +
				" IS \n" +
				" num_queries INTEGER; \n" +
				" queryId INTEGER; \n" +
				" loopq INTEGER; \n" +
				" loopc INTEGER; \n" +
				" oldbidt INTEGER; \n" +
				" oldctc FLOAT; \n" +
				" newbalance FLOAT; \n" +
				" tempbudget FLOAT; \n" +
				" cur_greedy TBid%rowtype; \n" +
				" rec_greedy TBid%rowtype; \n" +
				" BEGIN \n" +
				" 	DELETE Output; \n" +
				"	SELECT COUNT(*) \n" +
				"	INTO num_queries \n" +
				"	FROM Queries; \n" +
				"	FOR loopq IN 1..num_queries \n" +
				"	LOOP \n" +
				"		DELETE Tquery; \n" +
				"		DELETE TempQ; \n" +
				"		DELETE TBid; \n" +
				"		getQuery(loopq); \n" +		
				"		SELECT qId INTO queryId FROM TQuery WHERE rownum = 1; \n" +
				"		commit work; \n" +
				"		loopc := 0; \n" +
				"		FOR cur_greedy IN ( \n" +
				"		Select advertiserid, ROUND(ctc*similarity.cosine*totalbids * (1-exp(-balance/budget)), 9) as AdRank, totalbids  \n" +
				"		FROM( \n" +
				"			Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
				"	 		FROM \n" +
				"			((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
				"			From (Select advertiserId, count(keyword) as cnt \n" +
				"			From Keywords \n" +
				"			WHERE advertiserId IN (SELECT advertiserId \n" +
				"							       FROM Keywords \n" +
				"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
				"			Group by advertiserId,keyword order by advertiserid) Temp \n" +
				"			Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
				"			From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
				"			From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
				"			From TQuery q, Keywords k \n" +
				"			Where k.keyword = q.word ) KWDSTAT \n" +
				"			Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
				"		WHERE totalbids <= advertisers.balance \n" +
				"		ORDER BY adrank DESC, advertiserId ASC) \n" +
				"		LOOP \n" +
				"			loopc := loopc + 1; \n" +
				"			EXIT WHEN loopc > num_ads; \n" +	
				"			SELECT balance, budget, bidtime, ctc  \n" +
				"			INTO newbalance, tempbudget, oldbidt, oldctc \n" +
				"			FROM Advertisers \n" +
				"			WHERE Advertisers.advertiserId = cur_greedy.advertiserId; \n" +
				"			IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
				"			THEN \n" +
				"				newbalance := ROUND(newbalance - cur_greedy.totalbids, 2); \n" +
				"			END IF; \n" +
				"			INSERT INTO Output  \n" +
				"			VALUES(queryId, loopc, cur_greedy.advertiserId, newbalance, tempbudget); \n" +
				"			UPDATE Advertisers \n" +
				"			SET balance = newbalance, bidtime = oldbidt + 1 \n" +
				"			WHERE advertiserId = cur_greedy.advertiserId; \n" +
				"		END LOOP; \n" +
				"		commit work;			 \n" +
				"	END LOOP;	 \n" +
				" END; \n";
		
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}
	
	private void upProcGeedySecond() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doGreedySecond(num_ads IN INTEGER) \n" +
				" IS \n" +
				" num_queries INTEGER; \n" +
				" queryId INTEGER; \n" +
				" loopq INTEGER; \n" +
				" loopc INTEGER; \n" +
				" loopnum INTEGER; \n" +
				" oldbidt INTEGER; \n" +
				" oldctc FLOAT; \n" +
				" oldadrank FLOAT := 9999; \n" +
				" newbalance FLOAT; \n" +
				" tempbudget FLOAT; \n" +
				" finalbid FLOAT; \n" +
				" tempfloat FLOAT; \n" +
				" tempint INTEGER; \n" +
				" cur TBid%rowtype; \n" +
				" nxt TBid%rowtype; \n" +
				" BEGIN \n" +
				" DELETE Output; \n" +
				" SELECT COUNT(*) \n" +
				" INTO num_queries \n" +
				" FROM Queries; \n" +
				" dbms_output.put_line(num_queries); \n" +		
				" FOR loopq IN 1..num_queries \n" +
				" LOOP		 \n" +
				" 	DELETE Tquery; \n" +
				" 	DELETE TempQ; \n" +
				" 	DELETE TBid;		 \n" +
				" 	getQuery(loopq);		 \n" +
				" 	SELECT qId INTO queryId FROM TQuery WHERE rownum = 1;		 \n" +
				" 	commit work; \n" +
				" 	loopc := 0; \n" +
				" 	oldadrank := 9999; \n" +
				" 	INSERT INTO TBid ( \n" +
				" 	Select advertiserid, ROUND(ctc*similarity.cosine* totalbids, 9) as adrank, totalbids  \n" +
				" 	From( \n" +
				" 		Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
				"  		from \n" +
				" 		((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
				" 		From (Select advertiserId, count(keyword) as cnt \n" +
				" 		From Keywords \n" +
				"			WHERE advertiserId IN (SELECT advertiserId \n" +
				"							       FROM Keywords \n" +
				"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
				" 		Group by advertiserId,keyword order by advertiserid) Temp \n" +
				" 		Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
				" 		From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
				" 		From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
				" 		From TQuery q, Keywords k \n" +
				" 		Where k.keyword = q.word ) KWDSTAT \n" +
				" 		Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
				" 	WHERE totalbids <= advertisers.balance \n" +
				" 	--ORDER BY adrank DESC \n" +
				" 	);		 \n" +
				" 	IF SQL%NOTFOUND \n" +
				" 	THEN \n" +
				" 		CONTINUE; \n" +
				" 	END IF;		 \n" +
				" 	SELECT COUNT(*) \n" +
				" 	INTO tempint  \n" +
				" 	FROM TBid  \n" +
				" 	WHERE adrank < oldadrank;		 \n" +
				" 	IF tempint < num_ads \n" +
				" 	THEN \n" +
				" 		loopnum := tempint; \n" +
				" 	ELSE \n" +
				" 		loopnum := num_ads; \n" +
				" 	END IF;				 \n" +
				" 	FOR loopc IN 1..loopnum \n" +
				" 	LOOP \n" +
				" 		SELECT advertiserid, adrank, totalbids \n" +
				" 		INTO cur \n" +
				" 		FROM (SELECT advertiserid, adrank, totalbids, rownum AS m  \n" +
				" 			FROM ( SELECT *  \n" +
				" 				FROM tbid  \n" +
				" 				ORDER BY adrank desc, advertiserid asc)) 				 \n" +			
				" 		WHERE m = loopc;		 \n" +
				" 		oldadrank := cur.adrank;	 \n" +
				" 		SELECT COUNT(*) \n" +
				" 		INTO tempint \n" +
				" 		FROM TBid  \n" +
				" 		WHERE totalbids < cur.totalbids;	 \n" +
				" 		IF tempint = 0 \n" +
				" 		THEN \n" +
				" 			finalbid := cur.totalbids; \n" +
				" 		ELSE \n" +
				" 			SELECT * \n" +
				" 			INTO nxt \n" +
				" 			FROM( \n" +
				" 			SELECT *  \n" +
				" 			FROM Tbid \n" +
				" 			WHERE totalbids = (SELECT MAX(totalbids) \n" +
				" 				FROM Tbid \n" +
				" 				WHERE totalbids < cur.totalbids \n" +
				" 				)) \n" +
				" 			WHERE rownum = 1; \n" +
				" 			finalbid := nxt.totalbids; \n" +
				" 		END IF;	 \n" +
				" 		SELECT balance, budget, bidtime, ctc  \n" +
				" 		INTO newbalance, tempbudget, oldbidt, oldctc \n" +
				" 		FROM Advertisers \n" +
				" 		WHERE Advertisers.advertiserId = cur.advertiserId;		 \n" +
				" 		IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
				" 		THEN \n" +
				" 			newbalance := ROUND(newbalance - finalbid, 2); \n" +
				" 		END IF;		 \n" +
				" 		INSERT INTO Output  \n" +
				" 		VALUES(queryId, loopc, cur.advertiserId, newbalance, tempbudget);	 \n" +	
				" 		UPDATE Advertisers \n" +
				" 		SET balance = newbalance, bidtime = oldbidt + 1 \n" +
				" 		WHERE advertiserId = cur.advertiserId;		 \n" +
				" 	END LOOP; \n" +
				" 	commit work;					 \n" +
				" END LOOP;	 \n" +
				" END; \n";
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}

	private void upProcBalanceSecond() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doBalanceSecond(num_ads IN INTEGER) \n" +
				" IS \n" +
				" num_queries INTEGER; \n" +
				" queryId INTEGER; \n" +
				" loopq INTEGER; \n" +
				" loopc INTEGER; \n" +
				" loopnum INTEGER; \n" +
				" oldbidt INTEGER; \n" +
				" oldctc FLOAT; \n" +
				" oldadrank FLOAT := 9999; \n" +
				" newbalance FLOAT; \n" +
				" tempbudget FLOAT; \n" +
				" finalbid FLOAT; \n" +
				" tempfloat FLOAT; \n" +
				" tempint INTEGER; \n" +
				" cur TBid%rowtype; \n" +
				" nxt TBid%rowtype; \n" +
				" BEGIN \n" +
				" DELETE Output; \n" +
				" SELECT COUNT(*) \n" +
				" INTO num_queries \n" +
				" FROM Queries; \n" +
				" dbms_output.put_line(num_queries); \n" +		
				" FOR loopq IN 1..num_queries \n" +
				" LOOP		 \n" +
				" 	DELETE Tquery; \n" +
				" 	DELETE TempQ; \n" +
				" 	DELETE TBid;		 \n" +
				" 	getQuery(loopq);		 \n" +
				" 	SELECT qId INTO queryId FROM TQuery WHERE rownum = 1;		 \n" +
				" 	commit work; \n" +
				" 	loopc := 0; \n" +
				" 	oldadrank := 9999; \n" +
				" 	INSERT INTO TBid ( \n" +
				" 	Select advertiserid, ROUND(ctc*similarity.cosine* balance,9) as adrank, totalbids  \n" +
				" 	From( \n" +
				" 		Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
				"  		from \n" +
				" 		((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
				" 		From (Select advertiserId, count(keyword) as cnt \n" +
				" 		From Keywords \n" +
				"			WHERE advertiserId IN (SELECT advertiserId \n" +
				"							       FROM Keywords \n" +
				"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
				" 		Group by advertiserId,keyword order by advertiserid) Temp \n" +
				" 		Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
				" 		From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
				" 		From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
				" 		From TQuery q, Keywords k \n" +
				" 		Where k.keyword = q.word ) KWDSTAT \n" +
				" 		Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
				" 	WHERE totalbids <= advertisers.balance \n" +
				" 	--ORDER BY adrank DESC \n" +
				" 	);		 \n" +
				" 	IF SQL%NOTFOUND \n" +
				" 	THEN \n" +
				" 		CONTINUE; \n" +
				" 	END IF;		 \n" +
				" 	SELECT COUNT(*) \n" +
				" 	INTO tempint  \n" +
				" 	FROM TBid  \n" +
				" 	WHERE adrank < oldadrank;		 \n" +
				" 	IF tempint < num_ads \n" +
				" 	THEN \n" +
				" 		loopnum := tempint; \n" +
				" 	ELSE \n" +
				" 		loopnum := num_ads; \n" +
				" 	END IF;				 \n" +
				" 	FOR loopc IN 1..loopnum \n" +
				" 	LOOP \n" +
				" 		SELECT advertiserid, adrank, totalbids \n" +
				" 		INTO cur \n" +
				" 		FROM (SELECT advertiserid, adrank, totalbids, rownum AS m  \n" +
				" 			FROM ( SELECT *  \n" +
				" 				FROM tbid  \n" +
				" 				ORDER BY adrank desc, advertiserid asc)) 				 \n" +			
				" 		WHERE m = loopc;		 \n" +
				" 		oldadrank := cur.adrank;	 \n" +
				" 		SELECT COUNT(*) \n" +
				" 		INTO tempint \n" +
				" 		FROM TBid  \n" +
				" 		WHERE totalbids < cur.totalbids;	 \n" +
				" 		IF tempint = 0 \n" +
				" 		THEN \n" +
				" 			finalbid := cur.totalbids; \n" +
				" 		ELSE \n" +
				" 			SELECT * \n" +
				" 			INTO nxt \n" +
				" 			FROM( \n" +
				" 			SELECT *  \n" +
				" 			FROM Tbid \n" +
				" 			WHERE totalbids = (SELECT MAX(totalbids) \n" +
				" 				FROM Tbid \n" +
				" 				WHERE totalbids < cur.totalbids \n" +
				" 				)) \n" +
				" 			WHERE rownum = 1; \n" +
				" 			finalbid := nxt.totalbids; \n" +
				" 		END IF;	 \n" +
				" 		SELECT balance, budget, bidtime, ctc  \n" +
				" 		INTO newbalance, tempbudget, oldbidt, oldctc \n" +
				" 		FROM Advertisers \n" +
				" 		WHERE Advertisers.advertiserId = cur.advertiserId;		 \n" +
				" 		IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
				" 		THEN \n" +
				" 			newbalance := ROUND(newbalance - finalbid, 2); \n" +
				" 		END IF;		 \n" +
				" 		INSERT INTO Output  \n" +
				" 		VALUES(queryId, loopc, cur.advertiserId, newbalance, tempbudget);	 \n" +	
				" 		UPDATE Advertisers \n" +
				" 		SET balance = newbalance, bidtime = oldbidt + 1 \n" +
				" 		WHERE advertiserId = cur.advertiserId;		 \n" +
				" 	END LOOP; \n" +
				" 	commit work;					 \n" +
				" END LOOP;	 \n" +
				" END; \n";
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}
	
	private void upProcGeneralBalanceSecond() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE doGeneralBalanceSecond(num_ads IN INTEGER) \n" +
				" IS \n" +
				" num_queries INTEGER; \n" +
				" queryId INTEGER; \n" +
				" loopq INTEGER; \n" +
				" loopc INTEGER; \n" +
				" loopnum INTEGER; \n" +
				" oldbidt INTEGER; \n" +
				" oldctc FLOAT; \n" +
				" oldadrank FLOAT := 9999; \n" +
				" newbalance FLOAT; \n" +
				" tempbudget FLOAT; \n" +
				" finalbid FLOAT; \n" +
				" tempfloat FLOAT; \n" +
				" tempint INTEGER; \n" +
				" cur TBid%rowtype; \n" +
				" nxt TBid%rowtype; \n" +
				" BEGIN \n" +
				" DELETE Output; \n" +
				" SELECT COUNT(*) \n" +
				" INTO num_queries \n" +
				" FROM Queries; \n" +
				" dbms_output.put_line(num_queries); \n" +		
				" FOR loopq IN 1..num_queries \n" +
				" LOOP		 \n" +
				" 	DELETE Tquery; \n" +
				" 	DELETE TempQ; \n" +
				" 	DELETE TBid;		 \n" +
				" 	getQuery(loopq);		 \n" +
				" 	SELECT qId INTO queryId FROM TQuery WHERE rownum = 1;		 \n" +
				" 	commit work; \n" +
				" 	loopc := 0; \n" +
				" 	oldadrank := 9999; \n" +
				" 	INSERT INTO TBid ( \n" +
				" 	Select advertiserid, ROUND(ctc*similarity.cosine*totalbids *(1-exp(-balance/budget)), 9) as AdRank, totalbids  \n" +
				" 	From( \n" +
				" 		Select advertiserid, AiBi/(Ai*Bi) as cosine, totalbids \n" +
				"  		from \n" +
				" 		((Select temp.advertiserid, sqrt(sum(temp.cnt*temp.cnt)) as Bi \n" +
				" 		From (Select advertiserId, count(keyword) as cnt \n" +
				" 		From Keywords \n" +
				"			WHERE advertiserId IN (SELECT advertiserId \n" +
				"							       FROM Keywords \n" +
				"								   WHERE keyword IN (SELECT word FROM TQuery)) \n" +
				" 		Group by advertiserId,keyword order by advertiserid) Temp \n" +
				" 		Group by temp.advertiserid)  natural left outer join (Select sqrt(sum(times*times)) as Ai  \n" +
				" 		From Tquery) natural join (Select kwdstat.advertiserid, sum(kwdstat.times) as AiBi, sum(bids) as totalbids \n" +
				" 		From (Select k.advertiserId, k.keyword, q.times, k.bid as bids \n" +
				" 		From TQuery q, Keywords k \n" +
				" 		Where k.keyword = q.word ) KWDSTAT \n" +
				" 		Group by kwdstat.advertiserid ))) Similarity natural join advertisers \n" +
				" 	WHERE totalbids <= advertisers.balance \n" +
				" 	--ORDER BY adrank DESC \n" +
				" 	);		 \n" +
				" 	IF SQL%NOTFOUND \n" +
				" 	THEN \n" +
				" 		CONTINUE; \n" +
				" 	END IF;		 \n" +
				" 	SELECT COUNT(*) \n" +
				" 	INTO tempint  \n" +
				" 	FROM TBid  \n" +
				" 	WHERE adrank < oldadrank;		 \n" +
				" 	IF tempint < num_ads \n" +
				" 	THEN \n" +
				" 		loopnum := tempint; \n" +
				" 	ELSE \n" +
				" 		loopnum := num_ads; \n" +
				" 	END IF;				 \n" +
				" 	FOR loopc IN 1..loopnum \n" +
				" 	LOOP \n" +
				" 		SELECT advertiserid, adrank, totalbids \n" +
				" 		INTO cur \n" +
				" 		FROM (SELECT advertiserid, adrank, totalbids, rownum AS m  \n" +
				" 			FROM ( SELECT *  \n" +
				" 				FROM tbid  \n" +
				" 				ORDER BY adrank desc, advertiserid asc)) 				 \n" +			
				" 		WHERE m = loopc;		 \n" +
				" 		oldadrank := cur.adrank;	 \n" +
				" 		SELECT COUNT(*) \n" +
				" 		INTO tempint \n" +
				" 		FROM TBid  \n" +
				" 		WHERE totalbids < cur.totalbids;	 \n" +
				" 		IF tempint = 0 \n" +
				" 		THEN \n" +
				" 			finalbid := cur.totalbids; \n" +
				" 		ELSE \n" +
				" 			SELECT * \n" +
				" 			INTO nxt \n" +
				" 			FROM( \n" +
				" 			SELECT *  \n" +
				" 			FROM Tbid \n" +
				" 			WHERE totalbids = (SELECT MAX(totalbids) \n" +
				" 				FROM Tbid \n" +
				" 				WHERE totalbids < cur.totalbids \n" +
				" 				)) \n" +
				" 			WHERE rownum = 1; \n" +
				" 			finalbid := nxt.totalbids; \n" +
				" 		END IF;	 \n" +
				" 		SELECT balance, budget, bidtime, ctc  \n" +
				" 		INTO newbalance, tempbudget, oldbidt, oldctc \n" +
				" 		FROM Advertisers \n" +
				" 		WHERE Advertisers.advertiserId = cur.advertiserId;		 \n" +
				" 		IF MOD(oldbidt, 100) < (oldctc * 100) \n" +
				" 		THEN \n" +
				" 			newbalance := ROUND(newbalance - finalbid, 2); \n" +
				" 		END IF;		 \n" +
				" 		INSERT INTO Output  \n" +
				" 		VALUES(queryId, loopc, cur.advertiserId, newbalance, tempbudget);	 \n" +	
				" 		UPDATE Advertisers \n" +
				" 		SET balance = newbalance, bidtime = oldbidt + 1 \n" +
				" 		WHERE advertiserId = cur.advertiserId;		 \n" +
				" 	END LOOP; \n" +
				" 	commit work;					 \n" +
				" END LOOP;	 \n" +
				" END; \n";
		//System.out.println(sqlProc);
		stmt.executeUpdate(sqlProc);
		conn.commit();
	}
	
	private void upProcGetQuery() throws SQLException
	{
		String sqlProc = " CREATE or REPLACE PROCEDURE getQuery(line IN INTEGER) \n " +
			" IS \n " +
			" names VARCHAR2(400); \n " +
			" names_adjusted VARCHAR2(401); \n" +
			" word VARCHAR2(100); \n" +
			" comma_location NUMBER := 0; \n" +
			" prev_location NUMBER := 0; \n" +
			" BEGIN \n" +
			" SELECT query \n" +
			" INTO names \n" + 
			" FROM Queries \n" +
			" WHERE qid = line; \n" +
			" names_adjusted := names || ' '; \n" +
			" LOOP \n " +
			" comma_location := INSTR(names_adjusted,' ',comma_location+1); \n" +
			" EXIT WHEN comma_location = 0; \n" +
			" word := SUBSTR(names_adjusted,prev_location+1,comma_location-prev_location-1); \n" +
			" INSERT INTO TempQ VALUES(word); \n" +
			" prev_location := comma_location; \n" +
			" END LOOP; \n" +
			" INSERT INTO TQUERY (select qId, times, word \n" +
			"		from (SELECT word, COUNT(*) AS times FROM TempQ GROUP BY word) \n" +
			"		, \n" +
		    " 			(SELECT qId FROM Queries WHERE qId = line) \n" +
			"	); \n" +
			" END; \n";

		stmt.executeUpdate(sqlProc);
		conn.commit();
	}
	
	private void createQueriesTable() throws SQLException
	{
		String sqlCreate = "CREATE TABLE Queries " +
			"(qId INTEGER not NULL, " +
			" query VARCHAR(400) not NULL, " + 
			" PRIMARY KEY ( qId )) ";

		String sqlInsert = "INSERT INTO Queries VALUES(?,?)";
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() == 955){
				System.out.println("Table Queries already exists, skip creation.");
				return;
			}
			throw e;
		}
		System.out.println("Created table Queries");
		Scanner qrScanner;
		try{
			qrScanner = new Scanner(new FileInputStream(System.getProperty("user.dir") + "/" + NameQueries));
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);

		while(qrScanner.hasNextLine()){
			String ss = qrScanner.nextLine();
			String s1 = ss.substring(0, ss.indexOf("\t"));
			String s2 = ss.substring(ss.indexOf("\t") + 1);

			int qId = Integer.parseInt(s1);
			String query = s2;
			stmtInsert.clearParameters();
			stmtInsert.setInt(1, qId);
			stmtInsert.setString(2, query);
			stmtInsert.addBatch();
		}
		stmtInsert.executeBatch();
		conn.commit();
		qrScanner.close();
	}


	private void createKeywordsTable() throws SQLException
	{
		String sqlCreate = "CREATE TABLE Keywords " +
			"(advertiserId INTEGER not NULL, " +
			" keyword VARCHAR(100) not NULL, " + 
			" bid FLOAT, " + 
			" PRIMARY KEY ( advertiserId, keyword ), " +
			" FOREIGN KEY ( advertiserId ) REFERENCES Advertisers)"; 
		String sqlInsert = "INSERT INTO Keywords VALUES(?,?,?)";
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() == 955){
				System.out.println("Table Keywords already exists, skip creation.");
				return;
			}
			throw e;
		}
		System.out.println("Created table Keywords");
		Scanner kwdScanner;
		try{
			kwdScanner = new Scanner(new FileInputStream(System.getProperty("user.dir") + "/" + NameKeywords));
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);

		while(kwdScanner.hasNextLine()){
			String ss = kwdScanner.nextLine();
			String s1 = ss.substring(0, ss.indexOf("\t"));
			String s2 = ss.substring(ss.indexOf("\t") + 1);
			String s3 = s2.substring(0, s2.indexOf("\t"));
			String s4 = s2.substring(s2.indexOf("\t") + 1);

			int advertiserId = Integer.parseInt(s1);
			String keyword = s3;
			float bid = Float.parseFloat(s4);

			stmtInsert.clearParameters();
			stmtInsert.setInt(1, advertiserId);
			stmtInsert.setString(2, keyword);
			stmtInsert.setFloat(3, bid);
			stmtInsert.addBatch();
		}
		stmtInsert.executeBatch();
		conn.commit();
		kwdScanner.close();

	}

	private void createTempTables() throws SQLException
	{

		String sqlCreate = "CREATE TABLE TQuery " +
			" ( qId INTEGER , " + 
			" times INTEGER , " + 
			" word VARCHAR(100) )";
	
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() != 955)
			throw e;
		}
		
		sqlCreate = "CREATE TABLE TempQ " +
				"( word VARCHAR(100) )";
		
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() != 955)
			throw e;
		}
		
		sqlCreate = "CREATE TABLE TBid " +
				"( advertiserId INTEGER, adrank FLOAT, totalbids FLOAT )";
		
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() != 955)
			throw e;
		}
		
		sqlCreate = "CREATE TABLE Output " +
				"( qId INTEGER, " +
				" rank INTEGER, " +
				" advertiserId INTEGER, " +
				" balance FLOAT, " +
				" budget FLOAT )";
		
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() != 955)
			throw e;
		}
	}
	
	private void clearTables() throws SQLException
	{
		//Drop table TempQ;
		//Drop table Tquery;
		//Drop table Advertisers;
		//Drop table Keywords;
		//Drop table Queries;
		String sqlDrop;
		
		sqlDrop = "DROP TABLE TBid";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			System.out.println("error code" +e.getErrorCode());
			if(e.getErrorCode() != 942)
				throw e;
		}
		
		sqlDrop = "DROP TABLE TempQ";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			
			if(e.getErrorCode() != 942)
				throw e;
		}
			
		sqlDrop = "DROP TABLE TQuery";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			System.out.println(e.getErrorCode());
			if(e.getErrorCode() != 942)
				throw e;
		}
		
		sqlDrop = "DROP TABLE Output";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			System.out.println(e.getErrorCode());
			if(e.getErrorCode() != 942)
				throw e;
		}
		
		sqlDrop = "DROP TABLE Keywords";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			if(e.getErrorCode() != 942)
				throw e;
		}
		
		sqlDrop = "DROP TABLE Advertisers";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			if(e.getErrorCode() != 942)
				throw e;
		}
		
		sqlDrop = "DROP TABLE Queries";
		try{
			stmt.executeUpdate(sqlDrop);
		}catch(SQLException e){
			if(e.getErrorCode() != 942)
				throw e;
		}
	}
	
	private void restoreAdvertisers() throws SQLException
	{
		String sqlUpdate = "UPDATE Advertisers SET balance = budget, bidtime = 0 ";
		stmt.executeUpdate(sqlUpdate);
	}
	
	private void createAdvertisersTable() throws SQLException
	{
		String sqlCreate = "CREATE TABLE Advertisers " +
			"(advertiserId INTEGER not NULL, " +
			" bidtime INTEGER, " +
			" budget FLOAT, " + 
			" balance FLOAT, " +
			" ctc FLOAT, " + 
			" PRIMARY KEY ( advertiserId ))";
		String sqlInsert = "INSERT INTO Advertisers VALUES(?,?,?,?,?)";
		try{
			stmt.executeUpdate(sqlCreate);
		}catch(SQLException e){
			if(e.getErrorCode() == 955){
				System.out.println("Table Advertisers already exists, skip creation.");
				return;
			}
			throw e;
		}

		System.out.println("Created table Advertisers");
		Scanner advScanner;
		try{
			advScanner = new Scanner(new FileInputStream(System.getProperty("user.dir") + "/" + NameAdvertisers));
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}
		PreparedStatement stmtInsert = conn.prepareStatement(sqlInsert);
		while(advScanner.hasNextLine()){
			int advertiserId = advScanner.nextInt();
			float budget = advScanner.nextFloat();
			float ctc = advScanner.nextFloat();
			stmtInsert.clearParameters();
			stmtInsert.setInt(1, advertiserId);
			stmtInsert.setInt(2, 0);
			stmtInsert.setFloat(3, budget);
			stmtInsert.setFloat(4, budget);
			stmtInsert.setFloat(5, ctc);
			stmtInsert.addBatch();
		}
		stmtInsert.executeBatch();
		conn.commit();
		advScanner.close();
	}

	private void parseSystem()
	{
		String strptn1 = "(username = )([a-zA-Z0-9]*)";
		String strptn2 = "(password = )([a-zA-Z0-9]*)";
		String strptn3 = "(TASK)([0-9]*)(: num_ads = )([0-9]*)";
		String strptn = strptn1;
		Scanner system;
		try{
			system = new Scanner(new FileInputStream(System.getProperty("user.dir")+"/"+NameSystem));
		}catch(FileNotFoundException e){
			e.printStackTrace();
			return;
		}


		while(system.hasNextLine()){
			int i = 0;
			String line="";
			try{
				line = system.nextLine();
			}catch(Exception e){
				if(!(e instanceof EOFException))
					e.printStackTrace();
				//				break;
			}
			Pattern p = Pattern.compile(strptn);
			Matcher m = p.matcher(line);
			while(m.find()){
				if(strptn == strptn1){
					username = m.group(2);
				}else if(strptn == strptn2){
					password = m.group(2);
				}else{
					i = Integer.parseInt(m.group(2));
					numAds[i - 1] = Integer.parseInt(m.group(4));
				}
			}
			if(strptn == strptn1)
				strptn = strptn2;
			else if(strptn == strptn2)
				strptn = strptn3;
		}
		
		try{
			system.close();
		}catch(Exception e){
			e.printStackTrace();
		}
		return;
	}
	public String getUsername()
	{
		return username;
	}
	public String getPassword()
	{
		return password;
	}

	private PrintWriter []outputWriter;
	private Statement stmt = null;
	private Connection conn = null; 
	private String username = "";
	private String password = "";
	private int []numAds; 
	private static final String NameQueries = "Queries.dat";
	private static final String NameAdvertisers = "Advertisers.dat";
	private static final String NameKeywords = "Keywords.dat";
	private static final String NameSystem = "system.in";
	private static final String NameOutput = "system.out";

}

