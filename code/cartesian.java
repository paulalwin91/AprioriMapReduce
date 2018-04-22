import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

public class cartesian {
	public static List<String> frinput = new ArrayList<String>();

	public static void main(String[] args){
		List<List<String>> tempElements = new ArrayList<List<String>>();
//		List<String> ss = Arrays.asList("1,2,3".split(","));
		List<String> ssa = Arrays.asList("1,2,3".split(","));
		List<String> ssa1 = Arrays.asList("4,5,6".split(","));
		tempElements.add(ssa);
		tempElements.add(ssa);
		tempElements.add(ssa1);
		
		
		HashSet hs  = new HashSet(tempElements);
		if(tempElements.contains(Arrays.asList("3,2,1".split(",")))){
			System.out.println();
		}
		
//		List<List<String>> tempElements = new ArrayList<List<String>>();
//		
//		for(int i =0; i < 4; i++)
//		tempElements.add(ss);
//		
//		
//		
//		tempElements  = Lists.cartesianProduct(tempElements);
//		tempElements = ridSimilars(tempElements);
//		tempElements = sortAll(tempElements);
//		tempElements = removeDuplicatesProduct(tempElements);
		//tryStreams();
		System.out.println();
	}
//	
	public static List<List<String>> tryStreams(){
	
		List<List<String>> newself2 = new ArrayList<List<String>>();
		List<List<String>> newself = new ArrayList<List<String>>();
		
		newself2.add(Arrays.asList("1 2,3 4".split(",")));
		newself2.add(Arrays.asList("4 5,6 8".split(",")));
		
		List<List<String>> result = newself2.stream()
			    .map(list -> list.stream()
			            .flatMap(string -> Arrays.stream(string.split(" ")))
			            .collect(Collectors.toList()))
			    .collect(Collectors.toList());  
	
				//	ma forEach(list -> list.forEach(spcstr -> Arrays.asList(spcstr.split(" ")).forEach(str -> );););
	
				
		
		for (List<String> list : newself2) { // cartesian = [["1 2","3 4"],["4 5","6 8"]...] list = ["1 2","3 4"]...
			List<String> clearner = new ArrayList<String>();
			for (String string : list) { //string = "1 3 4 5"
				for (String stringElement : string.split(" ")) {
					clearner.add(stringElement);
				}
			}
			newself.add(clearner); //[["1","2","3","4"],["4","5","6","8"]...]
		}
//		
//		return newself;
////		
//		List<List<String>> newself = new ArrayList<List<String>>();
//		cartesian.stream().forEach(list -> list.forEach(spacedstr -> Arrays.asList(spacedstr.split(" ")).forEach(stringElement -> cleanProduct(cartesian) ); ););
//		cartesian.stream().forEach(list -> {			
//
//		});
//		
		return newself;		
	}
	
	public static List<List<String>> getCandidateSetsType1(List<String> singly, int setLength) {
		List<List<String>> elements = new ArrayList<List<String>>();
		for(int i = 0; i < setLength; i++)
			elements.add(singly);
			
		List<List<String>> lisy =  Lists.cartesianProduct(elements);
		List<List<String>> lisy1 =  cleanProductType1(lisy);
		return lisy1;
	}
	
	public static List<List<String>> getCandidateSets(List<List<String>> elements) {
		List<List<String>> lisy =  Lists.cartesianProduct(elements);
		List<List<String>> lisy1 =  cleanProduct(lisy);
		return lisy1;
	}

	public static List<List<String>> formatShizM(List<String[]> elements) {
		return cleanProduct(Lists
				.cartesianProduct(makeListofImmutable(elements)));
	}

	public static List<List<String>> removeDuplicatesProduct(
			List<List<String>> cartesian) {
		Set<List<String>> hs = new HashSet<>();
		hs.addAll(cartesian);
		List<List<String>> aa = new ArrayList<List<String>>();
		for (List<String> list : hs) {
			aa.add(list);
		}
		return aa;
	}

	public static List<String> fetchSingleton(String inp) {
		HashSet<String> hs = new HashSet<String>();
		for (String string : inp.split(",")) {
			for (String str : string.split(" ")) {
				hs.add(str);
			}
		}
		return new ArrayList<String>(hs);
	}

	public static List<List<String>> cleanProduct(List<List<String>> cartesian) {
//		splitProductGenx(cartesian);
		cartesian = splitProduct(cartesian);
		cartesian = ridSimilars(cartesian);
////		cartesian = sortAll(cartesian);
////		cartesian = removeDuplicatesProduct(cartesian);
		cartesian = hashsetit(cartesian);
		return cartesian;//filterNonFrequent(cartesian);
	}
	
	public static List<List<String>> cleanProductType1(List<List<String>> cartesian) {
		cartesian = ridSimilars(cartesian);
		cartesian = hashsetit(cartesian);
		return cartesian;
	}

	private static List<List<String>> hashsetit(List<List<String>> cartesian) {
		HashSet hs = new HashSet();
		for (List<String> list : cartesian) {
			List<String> ls = new ArrayList<String>(list);
			Collections.sort(ls);
			hs.add(ls);
		}
		return new ArrayList<List<String>>(hs);
	}
	private static List<List<String>> filterNonFrequent(
			List<List<String>> cartesian) {
		List<List<String>> aa = new ArrayList<List<String>>();
		for (List<String> list : cartesian) {
			Boolean shouldAdd = true;

			for (String str : list) {
				if (!frinput.contains(str)) {
					shouldAdd = false;
					break;
				}
			}
			if (shouldAdd)
				aa.add(list);
		}
		return aa;
	}

	private static List<List<String>> sortAll(List<List<String>> cartesian) {
		
		for (List<String> list : cartesian) {
			Collections.sort(list);
		}
		return cartesian;
	}

	private static List<List<String>> ridSimilars(List<List<String>> cartesian) {
		List<List<String>> l2 = new ArrayList<List<String>>();
		for (List<String> list : cartesian) {
			if (list.size() > 1) {
				boolean allEqual = new HashSet<String>(list).size() != list
						.size();
				if (!allEqual)
					l2.add(list);
			}
		}
		return l2;
	}

	public static List<List<String>> splitProductGenx(List<List<String>> cartesian) {
	List<List<String>> result = cartesian.stream()
		    .map(list -> list.stream()
		            .flatMap(string -> Arrays.stream(string.split(" ")))
		            .collect(Collectors.toList()))
		    .collect(Collectors.toList()); 
	return result;
	}
	
	public static List<List<String>> splitProduct(List<List<String>> cartesian) {
		List<List<String>> newself = new ArrayList<List<String>>();
		for (List<String> list : cartesian) { // cartesian = [["2 3","1 3 4 5"],["2 3","1 3 4 5"]...] list = ["2 3","1 3 4 5"]...
			List<String> clearner = new ArrayList<String>();
			for (String string : list) { //string = "1 3 4 5"
				for (String stringElement : string.split(" ")) {
					clearner.add(stringElement);
				}
			}
			newself.add(clearner);
		}	
		return newself;
	}

	private static List<ImmutableList<String>> makeListofImmutable(
			List<String[]> values) {
		List<ImmutableList<String>> converted = new LinkedList<>();
		values.forEach(array -> {
			converted.add(ImmutableList.copyOf(array));
		});
		return converted;
	}
}
