import asyncio
import time
import aiohttp
import asyncpg
from bs4 import BeautifulSoup


async def save_to_db(pool, title, date, article_url, author_name, user_profile_link):
    async with pool.acquire() as connection:
        try:
            query = """
            INSERT INTO articles_article (title, date, article_url, author_name, user_profile_link) 
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (article_url) DO NOTHING;
            """
            await connection.execute(query, title, date, article_url, author_name, user_profile_link)
        except Exception as e:
            print(f"Ошибка при сохранении статьи {article_url}: {e}")


async def article_exists(pool, article_url):
    async with pool.acquire() as connection:
        query = "SELECT 1 FROM articles_article WHERE article_url = $1"
        result = await connection.fetchrow(query, article_url)
        return result is not None


async def get_article_info(session, article_url, pool):
    try:
        if await article_exists(pool, article_url):
            print(f"Статья уже существует в базе данных: {article_url}")
            return

        async with session.get(article_url) as response:
            if response.status != 200:
                print(f"Ошибка при запросе {article_url}: статус-код {response.status}")
                return

            response_text = await response.text()
            soup = BeautifulSoup(response_text, 'lxml')

            title_tag = soup.find('h1', class_='tm-title_h1')
            title = title_tag.find('span').text if title_tag else 'Заголовок не найден'

            time_tag = soup.find('time')
            date = time_tag['title'] if time_tag and time_tag.has_attr('title') else 'Дата не найдена'

            author_tag = soup.find('a', class_='tm-user-info__username')
            author_name = author_tag.text.strip() if author_tag else 'Аноним'

            user_profile_tag = soup.find('a', class_='tm-user-info__userpic')
            user_profile_link = 'https://habr.com' + user_profile_tag.get('href') if user_profile_tag else 'Профиль не найден'

            print(f"Заголовок: {title}")
            print(f"Дата: {date}")
            print(f"Ссылка на статью: {article_url}")
            print(f"Автор: {author_name}")
            print(f"Профиль автора: {user_profile_link}")
            print("-" * 50)

            await save_to_db(pool, title, date, article_url, author_name, user_profile_link)

    except aiohttp.ClientError as e:
        print(f"Ошибка сети при запросе {article_url}: {e}")

    except Exception as e:
        print(f"Ошибка при обработке статьи {article_url}: {e}")


async def main():
    pool = await asyncpg.create_pool(
        user='postgres',
        password='123',
        database='habr_articles',
        host='localhost'
    )

    url = 'https://habr.com/ru/feed/'

    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as response:
                if response.status != 200:
                    print(f"Ошибка при запросе {url}: статус-код {response.status}")
                    return

                response_text = await response.text()
                soup = BeautifulSoup(response_text, 'lxml')
                articles = soup.find_all('article', class_='tm-articles-list__item')

                tasks = []
                for article in articles:
                    article_link_tag = article.find('a', class_='tm-title__link')
                    if article_link_tag:
                        article_url = 'https://habr.com' + article_link_tag.get('href')
                        tasks.append(get_article_info(session, article_url, pool))
                    else:
                        print("Ссылка на статью не найдена")

                await asyncio.gather(*tasks)

        except aiohttp.ClientError as e:
            print(f"Ошибка сети при запросе {url}: {e}")

        except Exception as e:
            print(f"Ошибка при обработке ленты: {e}")

    await pool.close()


while True:
    asyncio.run(main())
    time.sleep(600)
