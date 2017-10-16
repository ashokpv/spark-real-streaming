from dlink_pattern_matcher_v3 import DlinkPatternMatcherV3
import pandas

class DlinkGeneratorUtilityV2:
	DELIMITER = ['#123#', '.', '?', '/', '-', '+', ',', '_', '=', ' ']
	PATTERN_DELIM = "*123#"

	def __init__(self,dl_pattern_info,raw_data):
		self.dl_prefix = dl_pattern_info[0]
		self.dl_pattern = dl_pattern_info[1]
		self.raw_data = raw_data
		self.pattern_tokens = self.tokenize()
		
		self.generated_links = self.generate_links()

	def generate_links(self):
		deep_links = []


		for index_raw_data in range(0,len(self.raw_data)):
			link = self.dl_prefix
			for index in range(0,len(self.pattern_tokens)):
				link = "%s%s"%(link,self.append_token(self.pattern_tokens[index],self.raw_data.loc[index_raw_data]))
			deep_links.append(link)
		return deep_links


	def tokenize(self):
		tokens = self.dl_pattern.split(self.PATTERN_DELIM)
		tokens = list(filter(None,tokens))
		return tokens


	def append_token(self,pattern_token,listing):
		
		token_details = pattern_token.split(":")
		token_builder = ""
		# print("Token details:: %r"%(token_details))

		if(len(token_details) > 2):
			token = token_details[0]
			token_delim = token_details[1]
			key_start = int(token_details[2])
			distance_from_end = int(token_details[3])
			value_from_listing = listing[token]
			
			key_tokens = DlinkPatternMatcherV3.link_splitter(value_from_listing)
			final_token = DlinkPatternMatcherV3.build_string(key_tokens,key_start,len(key_tokens) - distance_from_end - 1)
			print("Final token: %r"%(final_token))
			token_builder = "%s%s"%(final_token,token_delim)
		else:
			token = token_details[0]
			token_delim = token_details[1]
			token_builder = "%s%s"%(token,token_delim)

		return token_builder



