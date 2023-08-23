package com.smallworld;

import java.io.File;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.Collectors;

import com.smallworld.model.TransactionModel;

public class TransactionDataFetcher {
	
	
	ObjectMapper objectMapper;
	List<TransactionModel> jsondata;
	public TransactionDataFetcher()
	{
		 objectMapper = new ObjectMapper();
		 File file = new File("transactions.json");
		 Scanner scan = new Scanner(file); 
		 String data = "";
		   while (scan.hasNextLine()) {
		           data += scan.nextLine();
		     } 
		 jsondata =  objectMapper.readValue(data, objectMapper.getTypeFactory().constructCollectionType(List.class, TransactionModel.class));
		 scan.close(); 
	} 
	 
      
    /**
     * Returns the sum of the amounts of all transactions
     */
    public double getTotalTransactionAmount() {
    	final double res = jsondata.stream()
    						.map(x -> x.getAmount())
							.collect(Collectors.summingDouble(Double::doubleValue));
    	return res; 
    }

    /**
     * Returns the sum of the amounts of all transactions sent by the specified client
     */
    public double getTotalTransactionAmountSentBy(String senderFullName) {
    	final double res = jsondata.stream()
							.filter(x -> x.getSenderFullName().equals(senderFullName))
							.map(x -> x.getAmount())
							.collect(Collectors.summingDouble(Double::doubleValue));
        return res;
    }

    /**
     * Returns the highest transaction amount
     */
    public double getMaxTransactionAmount() {
    	final double res = jsondata.stream()
						.map(x -> x.getAmount())
						.sorted(Comparator.reverseOrder())
						.findFirst().orElse((double) 0); 
    }

    /**
     * Counts the number of unique clients that sent or received a transaction
     */
    public long countUniqueClients() { 
    	return jsondata.stream()
				.map(x -> x.getMtn())
				.distinct()
				.collect(Collectors.toList()).size();
    }

    /**
     * Returns whether a client (sender or beneficiary) has at least one transaction with a compliance
     * issue that has not been solved
     */
    public boolean hasOpenComplianceIssues(String clientFullName) {
    	final long openIssues = jsondata.stream()
				.filter(x -> x.getBeneficiaryFullName().equals(clientFullName) || x.getSenderFullName().equals(clientFullName))
				.filter(x -> !x.isIssueSolved())
				.count();
    	return openIssues > 0 ? true : false;
    }

    /**
     * Returns all transactions indexed by beneficiary name
     */
    public Map<Object, Object> getTransactionsByBeneficiaryName() {
    	return jsondata.stream() 
    					.collect(Collectors.toMap(TransactionModel::getMtn, 
    							TransactionModel::getBeneficiaryFullName, (t1, t2)-> t1));
    }

    /**
     * Returns the identifiers of all open compliance issues
     */
    public Set<Long> getUnsolvedIssueIds() {
    	return jsondata.stream()
				.filter(x -> !x.isIssueSolved())
				.map(x -> x.getMtn())
				.collect(Collectors.toSet());
    }

    /**
     * Returns a list of all solved issue messages
     */
    public List<String> getAllSolvedIssueMessages() { 
    	return jsondata.stream().filter(x -> x.isIssueSolved())
				.map(x -> x.getIssueMessage()).collect(Collectors.toList());
    }

    /**
     * Returns the 3 transactions with highest amount sorted by amount descending
     */
    public List<Object> getTop3TransactionsByAmount() { 
    	return jsondata.stream()
    			.sorted(Comparator.comparingDouble(TransactionModel::getAmount).reversed())
    			.limit(3) 
    			.collect(Collectors.toList());
    }

    /**
     * Returns the sender with the most total sent amount
     */
    public Optional<Object> getTopSender() {
    	return Optional.ofNullable(jsondata.stream()
				.sorted(Comparator.comparingDouble(TransactionModel::getAmount).reversed())
				.limit(1).findFirst().orElse(null));
    }

}
