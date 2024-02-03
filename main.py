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
                first_crawl = data[0]["crawlInformation"].get("firstCrawl")
                last_crawl = data[0]["crawlInformation"].get("lastCrawl")

                # Se houver apenas o firstCrawl e tiver mais de 30 dias, atualiza o documento
                if first_crawl and not last_crawl:
                    first_crawl_date = datetime.strptime(first_crawl, "%Y-%m-%d %H:%M:%S")
                    if datetime.now() - first_crawl_date > timedelta(days=30):
                        return "update", data[0]["uid"]
                
                # Se houver lastCrawl e tiver mais de 30 dias desde o último crawl, atualiza o documento
                if last_crawl:
                    last_crawl_date = datetime.strptime(last_crawl, "%Y-%m-%d %H:%M:%S")
                    if datetime.now() - last_crawl_date > timedelta(days=30):
                        return "update", data[0]["uid"]

                # Se não precisar atualizar, retorna "skip" e o UID do documento
                return "skip", None

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
        # Limitar o número de tarefas executadas simultaneamente para 5
        tasks = [crawl(url) for url in queue[:5]]
        queue = queue[5:]  # Remover os URLs da fila que estão sendo processados agora

        # Executar as tarefas simultaneamente
        results = await asyncio.gather(*tasks)

        # Processar os resultados
        for result in results:
            crawl_result, internal_links = result

            if not crawl_result:
                continue

            current_url = crawl_result[0]["pageStructure"]["url"]

            if current_url in visited:
                continue

            visited.add(current_url)
            print("Crawling:", current_url)

            # Verificar se o URL está na base de dados do MeiliSearch
            action, existing_uid = await check_existing_url(current_url)

            if action == "skip":
                # Pular o URL, pois já foi rastreado recentemente ou não precisa ser atualizado
                print("Skipping URL:", current_url)
            elif action == "update" and existing_uid:
                # Atualizar o URL existente
                print("Updating existing URL:", current_url)

                # Remover o documento existente no MeiliSearch
                try:
                    index.delete_document(existing_uid)
                    print("Existing URL removed from MeiliSearch successfully.")
                except Exception as e:
                    print("Error removing existing URL from MeiliSearch:", e)

                # Adicionar o novo documento atualizado
                for crawl_result_item in crawl_result:
                    crawl_result_item['uid'] = existing_uid
                    try:
                        index.add_documents([crawl_result_item])
                        print("Updated crawl result sent to MeiliSearch successfully.")
                    except Exception as e:
                        print("Error sending updated crawl result to MeiliSearch:", e)
            else:
                # Enviar os resultados para o MeiliSearch
                for crawl_result_item in crawl_result:
                    crawl_result_item['uid'] = str(uuid.uuid4())  # Generating a unique UID for each document
                    try:
                        index.add_documents([crawl_result_item])
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
