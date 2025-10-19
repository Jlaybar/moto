import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

async function main() {
  try {
    // simple sanity: count users table
    const count = await prisma.user.count();
    console.log(`Prisma OK. Users count: ${count}`);

    // create a temp user if no users and env flag set
    if (process.env.SEED_DEMO === '1' && count === 0) {
      const u = await prisma.user.create({
        data: {
          email: 'demo@example.com',
          password: 'changeme',
        },
      });
      console.log('Seeded demo user id:', u.id);
    }
  } finally {
    await prisma.$disconnect();
  }
}

main().catch((e) => {
  console.error('Prisma check failed:', e?.message || e);
  process.exit(1);
});

