import asyncio
from crawler import crawl
import meilisearch
from datetime import datetime, timedelta
import json
import sys
import uuid

# Configure MeiliSearch
client = meilisearch.Client("https://ms-50b018410e2f-7405.sfo.meilisearch.io", "696af13e731da6b92dcf7a727dc9357b7ec0a8b4")
index = client.index("web-search")

async def check_existing_url(url):
    try:
        # Define o filtro para encontrar documentos com o URL específico
        filter_query = f"pageStructure.url = \"{url}\""

        # Faz a consulta na API do MeiliSearch
        response = index.search("", {"filter": filter_query, "limit": 1})
        data = response.get("hits", [])

        # Verifica se há resultados na resposta
        if data:
            # Obtém o URL do primeiro resultado retornado
            existing_url = data[0]["pageStructure"]["url"]

            # Verifica se o URL retornado é igual ao URL atual
            if existing_url == url:
                # Verificar se o primeiro rastreamento ocorreu há menos de um mês
                first_crawl = datetime.strptime(data[0]["crawlInformation"]["firstCrawl"], "%Y-%m-%d %H:%M:%S")
                if datetime.now() - first_crawl < timedelta(days=30):
                    # Retorna "skip" e o UID do documento
                    return "skip", data[0]["uid"]

        # Se não houver resultados ou o URL não corresponder, retorna "crawl"
        return "crawl", None
        
    except Exception as e:
        print("Error checking existing URL in MeiliSearch:", e)
        return "crawl", None


async def main():
    if len(sys.argv) < 2:
        urls = input("Please enter the URLs to crawl (separated by commas): ").split(",")
    else:
        urls = sys.argv[1:]

    visited = set()
    queue = urls

    while queue:
        current_url = queue.pop(0)

        if current_url in visited:
            continue

        visited.add(current_url)
        print("Crawling:", current_url)

        # Verificar se o URL está na base de dados do MeiliSearch
        action, existing_uid = await check_existing_url(current_url)

        if action == "skip":
            # Pular o URL, pois já foi rastreado recentemente
            print("Skipping URL as it was crawled recently:", current_url)
        elif action == "update" and existing_uid:
            # Atualizar o URL existente
            print("Updating existing URL:", current_url)
            # Crawl the current URL
            results, internal_links = await crawl(current_url)  # Alteração aqui

            # Extract internal links and add them to the queue for processing
            for result in results:
                for link in internal_links:
                    if link not in visited:
                        queue.append(link)

            # Obter o documento existente pelo UID
            existing_document = index.get_document(existing_uid)
            # Atualizar os dados do documento existente com os novos dados do rastreamento
            updated_document = {
                "uid": existing_uid,
                "crawlInformation": {
                    "firstCrawl": existing_document["crawlInformation"]["firstCrawl"],
                    "lastUpdate": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                },
                "pageMetadata": result["pageMetadata"],
                "pageStructure": result["pageStructure"]
            }
            # Atualizar o documento no MeiliSearch
            try:
                index.update_document(updated_document)
                print("Existing URL updated in MeiliSearch successfully.")
            except Exception as e:
                print("Error updating existing URL in MeiliSearch:", e)
        else:
            # Crawl the current URL
            results, internal_links = await crawl(current_url)  # Alteração aqui

            # Send results to MeiliSearch
            for result in results:
                result['uid'] = str(uuid.uuid4())  # Generating a unique UID for each document
                try:
                    index.add_documents([result])
                    print("Crawl result sent to MeiliSearch successfully.")
                except Exception as e:
                    print("Error sending crawl result to MeiliSearch:", e)

                # Extract internal links and add them to the queue for processing
                for link in internal_links:
                    if link not in visited:
                        queue.append(link)

        # Exibir os próximos 10 URLs na fila
        print("Next URLs in queue:")
        for i, next_url in enumerate(queue[:10], start=1):
            print(f"{i}. {next_url}")

if __name__ == "__main__":
    asyncio.run(main())
