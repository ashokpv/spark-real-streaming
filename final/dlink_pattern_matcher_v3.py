import json
import sys

DELIMITER = ['#123#', '.', '?', '/', '-', '+', ',', '_', '=', ' ']
PATTERN_DELIM = "*123#"

class DlinkPatternMatcherV3:

	@staticmethod
	def common_prefix(string1, string2):
		prefix = ""
		if len(string2) < len(string1):
			string1, string2 = string2, string1

		l1 = len(string1)
		for i in range(0, l1):
			if(string1[i] == string2[i]):
				prefix += string1[i]
			else:
				break

		return prefix

	@staticmethod
	def link_splitter(deep_link):
		token = ""
		arr = []
		index = 0
		for c in deep_link:
			if(c in DELIMITER):
				hsh = {}
				hsh['token'] = token
				hsh['delimiter'] = c
				hsh['index'] = index
				arr.append(hsh)
				token = ""
				index = index + 1
			else:
				token += c
		hsh = {}
		hsh['token'] = token
		hsh['index'] = index
		hsh['delimiter'] = ''
		arr.append(hsh)
		return arr

	@staticmethod
	def build_string(arr, start, last):
		raw = ""
		for index in range(start, last):
			raw += (arr[index]['token'] + arr[index]['delimiter'])

		raw += arr[last]['token']
		return raw

	@staticmethod
	def find_pattern_matches(tokens, patterns_hash, listings_json):
		token_size = len(tokens)
		for segment_length in reversed(range(1, token_size + 1)):
			DlinkPatternMatcherV3.update_pattern(tokens, patterns_hash, listings_json, segment_length)

	@staticmethod
	def update_pattern(tokens, patterns_hash, listings_json, segment_length):
		token_length = len(tokens)
		for index in range(0, token_length - segment_length + 1):
			segment = DlinkPatternMatcherV3.build_string(tokens, index, index + segment_length - 1)
			DlinkPatternMatcherV3.search_segment_in_json({'segment': segment, 'start': index, 'end': index + segment_length - 1, 'length': segment_length}, listings_json, patterns_hash)
				

	@staticmethod
	def search_segment_in_json(segment_info, listings_json, patterns_hash):
		segment = segment_info['segment']
		for listing_index, listing in enumerate(listings_json):
			for key in listing:
				value = str(listing[key])
				if segment.lower() in value.lower():
					# print("Found Match:: Segment: %s, Value: %s"%(segment,value))
					DlinkPatternMatcherV3.tokenize_and_match(key, value, segment_info, patterns_hash, listing_index)

	@staticmethod
	def tokenize_and_match(key, value, segment_info, patterns_hash, listing_index):
		value_tokens = DlinkPatternMatcherV3.link_splitter(value)
		segment_length = segment_info['length']
		for start_index in range(0, len(value_tokens) - segment_length + 1):
			built_string = DlinkPatternMatcherV3.build_string(value_tokens, start_index, start_index + segment_length - 1)
			if(built_string.lower() == segment_info['segment'].lower()):
				# print("Found match::::")
				hash_key = "%d..%d" % (segment_info['start'], segment_info['end'])
				if(not hash_key in patterns_hash):
					patterns_hash[hash_key] = []

				distance_from_end = len(value_tokens) - start_index - segment_length
				hsh = {
					'listing_id' : listing_index,
					'key' : key,
					'key_start' : start_index,
					'key_end' : start_index+segment_length-1,
					'distance_from_end' : distance_from_end
					}

				if(distance_from_end == 0 and start_index == 0):
					hsh['exactMatch'] = True
				
				patterns_hash[hash_key].append(hsh)



	@staticmethod
	def choose_dominant_listing(patterns_hash,tokens):
		token_size = len(tokens)
		hsh = {}
		for start_index in range(0,token_size):
			for end_index in range(start_index,token_size):
				hash_key = "%d..%d"%(start_index,end_index)
				if(hash_key in patterns_hash):
					patterns = patterns_hash[hash_key]
					for pattern in patterns:
						if(pattern['listing_id'] in hsh):
							hsh[pattern['listing_id']] += 1
						else:
							hsh[pattern['listing_id']] = 0


		mode_listing_id = max(hsh, key = hsh.get)

		for start_index in range(0,token_size):
			for end_index in range(start_index,token_size):
				hash_key = "%d..%d"%(start_index,end_index)
				if(hash_key in patterns_hash):
					patterns = patterns_hash[hash_key]
					switch = True
					current_pattern = {}
					for pattern in patterns:
						if(pattern['listing_id'] == mode_listing_id):
							switch = False
							if(not bool(current_pattern)):
								current_pattern = pattern
								patterns_hash[hash_key] = pattern
							if('exactMatch' in pattern and pattern['exactMatch'] == True):
								patterns_hash[hash_key] = pattern
								current_pattern = pattern
								break						

					if(switch == True):
						patterns_hash.pop(hash_key)


	@staticmethod
	def construct_pattern(patterns,patterns_hash,token_size):
		for segment_length in reversed(range(1,token_size+1)):
			for start_index in (0,token_size-segment_length+1):
				hash_key = "%d..%d"%(start_index,start_index+segment_length-1)
				if((hash_key in patterns_hash) and not DlinkPatternMatcherV3.check_if_already_matched(patterns,start_index,start_index+segment_length-1)):
					DlinkPatternMatcherV3.modify_pattern_arr(patterns,patterns_hash[hash_key],start_index,start_index+segment_length-1)

		final_arr = []
		index = 0
		while(index < len(patterns)):
			curr_element = patterns[index]
			last_element = curr_element
			index += 1
			while(index < len(patterns) and patterns[index]['token'] == curr_element['token']):
				last_element = patterns[index]
				index += 1

			final_arr.append(curr_element)
			final_arr[-1]['delimiter'] = last_element['delimiter']

		return final_arr


	@staticmethod
	def modify_pattern_arr(patterns,key_hash,start_index,end_index):
		
		# print("Key Hash:::%r"%(key_hash))

		for index in range(start_index,end_index+1):
			hsh = {}
			hsh['matched'] = True
			hsh['token'] = key_hash['key']
			hsh['start_index'] = start_index
			hsh['end_index'] = end_index
			hsh['key_start'] = key_hash['key_start']
			hsh['key_end'] = key_hash['key_end']
			hsh['distance_from_end'] = key_hash['distance_from_end']
			hsh['delimiter'] = patterns[index]['delimiter']
			patterns[index] = hsh


	@staticmethod
	def check_if_already_matched(patterns,start_index,end_index):
		for index in range(start_index,end_index+1):
			if(type(patterns[index]) is dict and 'matched' in patterns[index] and patterns[index]['matched'] == True):
				return True
			else:
				return False

	@staticmethod
	def choose_mode_pattern(generated_patterns):
		hsh = {}
		for pattern in generated_patterns:
			if(pattern['link'] in hsh):
				hsh[pattern['link']] += 1
			else:
				hsh[pattern['link']] = 1

		mode_pattern_link = max(hsh, key = hsh.get)

		for pattern in generated_patterns:
			if(pattern['link'] == mode_pattern_link):
				return pattern

	@staticmethod
	def generate_dl_pattern(deep_links,listings_json):
		
		dl_prefix = deep_links[0]
		for deep_link in deep_links:
			dl_prefix = DlinkPatternMatcherV3.common_prefix(dl_prefix,deep_link)

		nLinks = len(deep_links)
		for i in reversed(range(0,len(dl_prefix))):
			if(dl_prefix[i] == '/'):
				dl_prefix = dl_prefix[:i+1]
				break

		for index in range(0,len(deep_links)):
			deep_links[index] = deep_links[index].strip()
			deep_links[index] = deep_links[index][len(dl_prefix):]


		generated_patterns = []


		for deep_link in deep_links:

			tokens = DlinkPatternMatcherV3.link_splitter(deep_links[0])
			patterns = list(tokens)
			patterns_hash = {}

			DlinkPatternMatcherV3.find_pattern_matches(tokens,patterns_hash,listings_json)
			DlinkPatternMatcherV3.choose_dominant_listing(patterns_hash,tokens)

			final_arr = DlinkPatternMatcherV3.construct_pattern(patterns,patterns_hash,len(tokens))


			link_builder = ""

			for element in final_arr:
				if('matched' in element and element['matched'] == True):
					# print("ElementToken%s"%(element['token']))
					# print("ElementDelim%s"%(element['delimiter']))
					link_builder = "%s%s%s:%s:%d:%d%s"%(link_builder,PATTERN_DELIM,element['token'],element['delimiter'],element['key_start'],element['distance_from_end'],PATTERN_DELIM)
					# link_builder = "%s<%s[%d..length-%d]>%s"%(link_builder,element['token'],element['key_start'],element['distance_from_end'],element['delimiter'])
				else:
					# link_builder ="%s<'%s'>%s"%(link_builder,element['token'],element['delimiter']) 
					link_builder = "%s%s%s:%s%s"%(link_builder,PATTERN_DELIM,element['token'],element['delimiter'],PATTERN_DELIM)


			

			generated_patterns.append({"link":link_builder,"pattern_array":final_arr})


		final_pattern = DlinkPatternMatcherV3.choose_mode_pattern(generated_patterns)

		# print("Final Link: %s"%(final_pattern['link']))
		return [dl_prefix,final_pattern['link']]





